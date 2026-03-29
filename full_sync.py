#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sincronização completa do catálogo Metabooks (LA=por) com o PostgreSQL.

Pagina todos os ~200k livros portugueses da Metabooks e faz upsert direto
no PostgreSQL, sem passar pelo Directus API (mais rápido, sem revisões).

Variáveis de ambiente:
  DATABASE_URL       postgresql://...  (Railway PostGIS)
  DIRECTUS_URL       https://directus-production-afdd.up.railway.app
  DIRECTUS_TOKEN     token do usuário
  META_TOKEN         token da API Metabooks
  META_COVER_TOKEN   token de capa (opcional)
  SYNC_LOG_ID        ID de entrada existente em sync_log (para resume)
"""

import json
import os
import re
import sys
if sys.stdout.encoding != 'utf-8':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print('ERRO: psycopg2-binary não instalado. Execute: pip install psycopg2-binary', file=sys.stderr)
    sys.exit(1)

# ── Configuração ──────────────────────────────────────────────────────────────

DATABASE_URL     = os.environ['DATABASE_URL']
DIRECTUS_URL     = os.environ.get('DIRECTUS_URL', 'https://directus-production-afdd.up.railway.app')
DIRECTUS_TOKEN   = os.environ['DIRECTUS_TOKEN']
META_TOKEN       = os.environ['META_TOKEN']
META_BASE        = 'https://www.metabooks.com/api/v2'
META_COVER_TOKEN = os.environ.get('META_COVER_TOKEN', '')
SYNC_LOG_ID      = os.environ.get('SYNC_LOG_ID', '')
PAGE_SIZE        = 100
CHECKPOINT_EVERY = 50   # páginas entre cada checkpoint/verificação de cancelar

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_meta(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'livros-wiki/1.0'})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise e


def directus(method, path, data=None):
    url = f'{DIRECTUS_URL}{path}'
    headers = {'Authorization': f'Bearer {DIRECTUS_TOKEN}', 'Content-Type': 'application/json'}
    body = json.dumps(data).encode() if data is not None else None
    req  = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        print(f'  Directus {e.code}: {e.read().decode()[:200]}', file=sys.stderr)
        return None


def strip_html(s):
    return re.sub(r'<[^>]+>', '', s or '').strip()


def parse_date(s):
    if not s:
        return None
    if '/' in s:
        parts = s.split('/')
        if len(parts) == 3:
            d, m, y = parts
            return f'{y}-{m.zfill(2)}-{d.zfill(2)}'
    return s[:10] if len(s) >= 10 else None


def normalize_pub(s):
    s = unicodedata.normalize('NFD', s.strip().lower())
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    s = re.sub(r'^(editora|editores|ed\.|grupo editorial|grupo)\s+', '', s)
    s = re.sub(r'\s+(editora|editores|livros|books|ltda\.?|s\.?a\.?)$', '', s)
    s = re.sub(r'[^\w\s]', '', s)
    return re.sub(r'\s+', ' ', s).strip()


# ── 1. Garantir campos no sync_log e biblioteca ────────────────────────────────

print('Verificando campos em sync_log...')
fields_resp = directus('GET', '/fields/sync_log')
existing_fields = {f['field'] for f in (fields_resp or {}).get('data', [])}

for fname, fdef in [
    ('pagina_atual',  {'field': 'pagina_atual',  'type': 'integer', 'meta': {'hidden': False}, 'schema': {'is_nullable': True}}),
    ('total_paginas', {'field': 'total_paginas', 'type': 'integer', 'meta': {'hidden': False}, 'schema': {'is_nullable': True}}),
    ('cancelar',      {'field': 'cancelar',      'type': 'boolean', 'meta': {'hidden': False}, 'schema': {'is_nullable': True, 'default_value': False}}),
    ('tipo',          {'field': 'tipo',           'type': 'string',  'meta': {'hidden': False}, 'schema': {'is_nullable': True}}),
]:
    if fname not in existing_fields:
        print(f'  Criando campo {fname}...')
        directus('POST', '/fields/sync_log', fdef)
    else:
        print(f'  {fname} OK')

print('Verificando campos em biblioteca...')
bib_fields_resp = directus('GET', '/fields/biblioteca')
bib_existing = {f['field'] for f in (bib_fields_resp or {}).get('data', [])}

for fname, fdef in [
    ('date_created', {
        'field': 'date_created', 'type': 'dateTime',
        'meta': {'special': ['date-created'], 'hidden': True},
        'schema': {'is_nullable': True},
    }),
    ('date_updated', {
        'field': 'date_updated', 'type': 'dateTime',
        'meta': {'special': ['date-updated'], 'hidden': True},
        'schema': {'is_nullable': True},
    }),
]:
    if fname not in bib_existing:
        print(f'  Criando campo {fname} em biblioteca...')
        directus('POST', '/fields/biblioteca', fdef)
    else:
        print(f'  biblioteca.{fname} OK')

# ── 2. Conectar ao banco ──────────────────────────────────────────────────────

print('\nConectando ao banco de dados...')
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = False
cur  = conn.cursor()
print('  Conectado')

# ── 3. Carregar ISBNs existentes ──────────────────────────────────────────────

print('\nCarregando ISBNs existentes...')
cur.execute("SELECT isbn FROM biblioteca WHERE isbn IS NOT NULL AND isbn != ''")
existing_isbns = {row[0] for row in cur.fetchall()}
print(f'  {len(existing_isbns):,} ISBNs no banco')

# ── 4. Carregar selos existentes ──────────────────────────────────────────────

print('\nCarregando selos...')
selos_by_mbid = {}
selos_by_name = {}

resp = directus('GET', '/items/selos?fields=id,nome_display,search_metabooks,publisher_mb_id&limit=500')
selos_data = (resp or {}).get('data', [])
for s in selos_data:
    sid = s['id']
    if s.get('publisher_mb_id'):
        selos_by_mbid[s['publisher_mb_id']] = sid
    if s.get('nome_display'):
        selos_by_name[normalize_pub(s['nome_display'])] = sid
    if s.get('search_metabooks'):
        selos_by_name[normalize_pub(s['search_metabooks'])] = sid
print(f'  {len(selos_data):,} selos carregados')


def get_or_create_selo(pub_name, mb_id):
    """Retorna ID do selo (existente ou criado como inativo via Directus)."""
    if mb_id and mb_id in selos_by_mbid:
        return selos_by_mbid[mb_id]
    norm = normalize_pub(pub_name) if pub_name else ''
    if norm and norm in selos_by_name:
        return selos_by_name[norm]
    if not pub_name:
        return None
    r = directus('POST', '/items/selos', {
        'nome_display':     pub_name,
        'search_metabooks': pub_name,
        'publisher_mb_id':  mb_id or None,
        'ativo':            False,
    })
    if r and r.get('data'):
        new_id = r['data']['id']
        if mb_id:
            selos_by_mbid[mb_id] = new_id
        if norm:
            selos_by_name[norm] = new_id
        print(f'  + Novo selo: {pub_name} (id={new_id})')
        return new_id
    return None


# ── 5. Verificar/criar sync_log ───────────────────────────────────────────────

start_page         = 0
livros_criados     = 0
livros_atualizados = 0

if SYNC_LOG_ID:
    r = directus('GET', f'/items/sync_log/{SYNC_LOG_ID}')
    log = (r or {}).get('data', {})
    if log.get('status') in ('paused', 'running') and log.get('pagina_atual') is not None:
        start_page         = int(log['pagina_atual'])
        livros_criados     = int(log.get('livros_criados') or 0)
        livros_atualizados = int(log.get('livros_atualizados') or 0)
        print(f'\nRetomando da página {start_page} '
              f'({livros_criados} criados, {livros_atualizados} atualizados)')
    directus('PATCH', f'/items/sync_log/{SYNC_LOG_ID}', {
        'status':  'running',
        'tipo':    'full_sync',
        'cancelar': False,
    })
else:
    log_res = directus('POST', '/items/sync_log', {
        'iniciado_em': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'status':      'running',
        'tipo':        'full_sync',
        'cancelar':    False,
    })
    if log_res and log_res.get('data'):
        SYNC_LOG_ID = str(log_res['data']['id'])
        print(f'sync_log criado: id={SYNC_LOG_ID}')

# ── 6. Paginar Metabooks (LA=por) ─────────────────────────────────────────────

q           = urllib.parse.quote('LA=por')
total_pages = 0
page        = start_page
cancelled   = False
new_selos   = 0

print(f'\nPaginando catálogo Metabooks a partir da página {page}...')

while True:
    url = (f'{META_BASE}/products?access_token={META_TOKEN}'
           f'&search={q}&size={PAGE_SIZE}'
           f'&sort=publicationDate&direction=desc&page={page}')

    try:
        j = fetch_meta(url)
    except Exception as e:
        print(f'  ERRO página {page}: {e}', file=sys.stderr)
        time.sleep(5)
        continue

    items = j.get('content', [])
    if not items:
        print(f'  Sem resultados na página {page} — fim do catálogo')
        break

    # Registrar totais na primeira página
    if not total_pages and j.get('totalElements'):
        total_el    = j['totalElements']
        total_pages = j.get('totalPages') or (total_el // PAGE_SIZE + 1)
        print(f'  Total: {total_el:,} livros, ~{total_pages:,} páginas')
        if SYNC_LOG_ID:
            directus('PATCH', f'/items/sync_log/{SYNC_LOG_ID}', {
                'total_paginas':  total_pages,
                'total_editoras': total_el,
                'pagina_atual':   page,
                'progresso_msg':  f'Iniciando… {total_el:,} livros em {total_pages:,} páginas',
            })

    # ── Processar itens da página ─────────────────────────────────────────────
    new_rows = []
    upd_rows = []

    for b in items:
        if b.get('productType') != 'pbook':
            continue
        isbn = (b.get('gtin') or b.get('isbn') or '').replace('-', '').replace(' ', '')
        if not isbn:
            continue

        pub_name = (b.get('publisherName') or b.get('publisher') or '').strip()
        mb_id    = b.get('publisherMbId') or b.get('publisherId') or ''
        capa_url = (f"{b['coverUrl']}?access_token={META_COVER_TOKEN}"
                    if b.get('coverUrl') and META_COVER_TOKEN else b.get('coverUrl') or '')

        contributors = [
            c for c in (b.get('contributors') or [])
            if c.get('firstName') or c.get('lastName') or c.get('groupName')
        ]

        values = (
            (b.get('title') or '—')[:500],
            (b.get('author') or '')[:255],
            pub_name[:255] if pub_name else '',
            capa_url,
            strip_html(b.get('mainDescription') or b.get('shortDescription')),
            strip_html(b.get('biographicalNote')),
            psycopg2.extras.Json(contributors) if contributors else None,
            parse_date(b.get('publicationDate') or b.get('onSaleDate')),
            isbn,
        )

        if isbn in existing_isbns:
            upd_rows.append(values)
        else:
            new_rows.append(values)
            existing_isbns.add(isbn)
            # Auto-criar selo se não existe
            if pub_name:
                prev = len(selos_by_mbid) + len(selos_by_name)
                get_or_create_selo(pub_name, mb_id)
                if len(selos_by_mbid) + len(selos_by_name) > prev:
                    new_selos += 1

    # ── Inserir novos ─────────────────────────────────────────────────────────
    if new_rows:
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO biblioteca
              (titulo, autor, editora, capa_url, sinopse, biografia_autor,
               contributors, data_publicacao, isbn, status, date_created, date_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'published', NOW(), NOW())
        """, new_rows)
        livros_criados += len(new_rows)

    # ── Atualizar existentes ──────────────────────────────────────────────────
    if upd_rows:
        psycopg2.extras.execute_batch(cur, """
            UPDATE biblioteca SET
              titulo = %s, autor = %s, editora = %s, capa_url = %s,
              sinopse = %s, biografia_autor = %s,
              contributors = %s, data_publicacao = %s,
              date_updated = NOW()
            WHERE isbn = %s
        """, upd_rows)
        livros_atualizados += len(upd_rows)

    conn.commit()
    page += 1

    # ── Progresso leve a cada 10 páginas ─────────────────────────────────────
    if SYNC_LOG_ID and page % 10 == 0:
        pct = round(page / total_pages * 100) if total_pages else 0
        directus('PATCH', f'/items/sync_log/{SYNC_LOG_ID}', {
            'pagina_atual':         page,
            'editoras_processadas': page,
            'progresso_msg':        f'Página {page}/{total_pages or "?"} ({pct}%) — {livros_criados:,} criados, {livros_atualizados:,} atualizados',
            'livros_criados':       livros_criados,
            'livros_atualizados':   livros_atualizados,
        })

    # ── Checkpoint completo a cada N páginas (verifica cancelar) ─────────────
    if page % CHECKPOINT_EVERY == 0:
        pct = round(page / total_pages * 100) if total_pages else 0
        msg = (f'Página {page}/{total_pages or "?"} ({pct}%) — '
               f'{livros_criados:,} criados, {livros_atualizados:,} atualizados')
        print(msg)
        if SYNC_LOG_ID:
            directus('PATCH', f'/items/sync_log/{SYNC_LOG_ID}', {
                'pagina_atual':         page,
                'editoras_processadas': page,
                'progresso_msg':        msg,
                'livros_criados':       livros_criados,
                'livros_atualizados':   livros_atualizados,
            })
            # Verificar flag de cancelamento
            r = directus('GET', f'/items/sync_log/{SYNC_LOG_ID}?fields=cancelar')
            if r and r.get('data', {}).get('cancelar'):
                print('  Flag cancelar detectado — pausando')
                cancelled = True
                break

    if j.get('last', True):
        print(f'  Última página: {page - 1}')
        break

# ── 7. Finalizar ──────────────────────────────────────────────────────────────

cur.close()
conn.close()

now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
status  = 'paused' if cancelled else 'concluido'

print(f'\n{"Pausado" if cancelled else "Concluído"}: '
      f'{livros_criados:,} criados, {livros_atualizados:,} atualizados, '
      f'{new_selos} novos selos')

if SYNC_LOG_ID:
    patch = {
        'status':             status,
        'pagina_atual':       page,
        'livros_criados':     livros_criados,
        'livros_atualizados': livros_atualizados,
        'progresso_msg': (
            f'Pausado na pág. {page} — {livros_criados:,} criados, {livros_atualizados:,} atualizados'
            if cancelled else
            f'Concluído — {livros_criados:,} criados, {livros_atualizados:,} atualizados'
        ),
    }
    if not cancelled:
        patch['finalizado_em'] = now_str
    directus('PATCH', f'/items/sync_log/{SYNC_LOG_ID}', patch)
    print(f'sync_log {SYNC_LOG_ID} → {status}')
