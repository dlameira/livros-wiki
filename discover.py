#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Descobre editoras de livros brasileiros na Metabooks e adiciona ao Directus
como selos inativos (ativo=False) para posterior ativação manual.

Variáveis de ambiente:
  DIRECTUS_URL     https://directus-production-afdd.up.railway.app
  DIRECTUS_TOKEN   token do usuário
  META_TOKEN       token da API Metabooks
  SAMPLE_PAGES     páginas a amostrar (default 500 → 50k livros)
  MIN_BOOKS        mínimo de aparições na amostra para incluir (default 3)
  DISCOVER_LOG_ID  ID do registro em sync_log (opcional)
"""

import json
import os
import sys
if sys.stdout.encoding != 'utf-8':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

DIRECTUS_URL    = os.environ.get('DIRECTUS_URL', 'https://directus-production-afdd.up.railway.app')
DIRECTUS_TOKEN  = os.environ['DIRECTUS_TOKEN']
META_TOKEN      = os.environ['META_TOKEN']
META_BASE       = 'https://www.metabooks.com/api/v2'
SAMPLE_PAGES    = int(os.environ.get('SAMPLE_PAGES', '500'))
MIN_BOOKS       = int(os.environ.get('MIN_BOOKS', '3'))
DISCOVER_LOG_ID = os.environ.get('DISCOVER_LOG_ID', '')


def meta_get(url):
    req = urllib.request.Request(url, headers={'User-Agent': 'livros-wiki/1.0'})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


def directus(method, path, data=None):
    url = f'{DIRECTUS_URL}{path}'
    headers = {'Authorization': f'Bearer {DIRECTUS_TOKEN}', 'Content-Type': 'application/json'}
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        print(f'  Directus HTTP {e.code}: {e.read().decode()[:300]}', file=sys.stderr)
        return None


# ── 0. Garantir campo total_livros_mb no Directus ────────────────────────────

print('Verificando campo total_livros_mb em selos...')
fields_resp = directus('GET', '/fields/selos')
existing_fields = {f['field'] for f in (fields_resp or {}).get('data', [])}
if 'total_livros_mb' not in existing_fields:
    print('  Criando campo total_livros_mb...')
    directus('POST', '/fields/selos', {
        'field': 'total_livros_mb',
        'type':  'integer',
        'meta': {'hidden': False, 'interface': 'input', 'readonly': True,
                 'note': 'Contagem estimada de livros na Metabooks'},
        'schema': {'is_nullable': True},
    })
    print('  Campo criado.')
else:
    print('  Campo já existe.')

# ── 1. Selos já existentes no Directus ────────────────────────────────────────

print('\nCarregando selos existentes no Directus...')
resp = directus('GET', '/items/selos?fields=nome_display,search_metabooks,publisher_mb_id&limit=500')
existing_names  = set()
existing_mb_ids = set()
if resp and resp.get('data'):
    for s in resp['data']:
        if s.get('nome_display'):
            existing_names.add(s['nome_display'].strip().lower())
        if s.get('search_metabooks'):
            existing_names.add(s['search_metabooks'].strip().lower())
        if s.get('publisher_mb_id'):
            existing_mb_ids.add(s['publisher_mb_id'])
print(f'  {len(existing_names)} nomes/buscas já no Directus\n')

# ── 2. Amostrar Metabooks ─────────────────────────────────────────────────────

print(f'Amostrando {SAMPLE_PAGES} páginas da Metabooks (LA=por)...')
publishers = {}   # key → {'name': str, 'mb_id': str|None, 'count': int}
q = urllib.parse.quote('LA=por')
total_catalog = 0  # totalElements from Metabooks

half = SAMPLE_PAGES // 2
total_pages_done = 0

for direction, pages in [('desc', half), ('asc', SAMPLE_PAGES - half)]:
    print(f'  Direção: {direction} ({pages} páginas)')
    for page in range(pages):
        if page % 50 == 0:
            pct = total_pages_done / SAMPLE_PAGES * 100
            print(f'    Página {total_pages_done}/{SAMPLE_PAGES} ({pct:.0f}%) — {len(publishers)} editoras únicas')
            if DISCOVER_LOG_ID:
                directus('PATCH', f'/items/sync_log/{DISCOVER_LOG_ID}', {
                    'progresso_msg':        f'Amostrando… {total_pages_done}/{SAMPLE_PAGES} págs, {len(publishers)} editoras',
                    'editoras_processadas': total_pages_done,
                    'total_editoras':       SAMPLE_PAGES,
                })

        url = (f'{META_BASE}/products?access_token={META_TOKEN}'
               f'&search={q}&size=100&sort=publicationDate&direction={direction}&page={page}')
        try:
            j = meta_get(url)
            if not total_catalog and j.get('totalElements'):
                total_catalog = j['totalElements']
                print(f'    Total catálogo Metabooks: {total_catalog:,}')
            items = j.get('content', [])
            if not items:
                print(f'    Sem resultados na página {page} ({direction})')
                break
            for b in items:
                if b.get('productType') != 'pbook':
                    continue
                mb_id = b.get('publisherMbId') or b.get('publisherId') or ''
                name  = (b.get('publisherName') or b.get('publisher') or '').strip()
                if not name:
                    continue
                key = mb_id if mb_id else name.lower()
                if key not in publishers:
                    publishers[key] = {'name': name, 'mb_id': mb_id or None, 'count': 0}
                publishers[key]['count'] += 1
            if j.get('last', True):
                print(f'    Última página em {page} ({direction})')
                break
        except Exception as e:
            print(f'    Erro na página {page}: {e}', file=sys.stderr)

        total_pages_done += 1

total_unique = len(publishers)
print(f'\n{total_unique} editoras únicas encontradas na amostra')

# Fator de extrapolação: estimativa de livros reais por editora
books_sampled = total_pages_done * 100
extrap = (total_catalog / books_sampled) if books_sampled and total_catalog else 1.0
print(f'Fator de extrapolação: {extrap:.1f}x  ({books_sampled:,} amostrados / {total_catalog:,} total)\n')

# ── 3. Filtrar desconhecidas com livros suficientes ───────────────────────────

to_add = []
for key, info in publishers.items():
    if info['count'] < MIN_BOOKS:
        continue
    name_lower = info['name'].lower()
    if name_lower in existing_names:
        continue
    if info['mb_id'] and info['mb_id'] in existing_mb_ids:
        continue
    estimated = max(1, round(info['count'] * extrap))
    to_add.append({**info, 'estimated': estimated})

to_add.sort(key=lambda x: x['estimated'], reverse=True)
print(f'{len(to_add)} novas editoras (≥{MIN_BOOKS} livros na amostra) para adicionar\n')

# ── 4. Criar no Directus ──────────────────────────────────────────────────────

added = 0
for info in to_add:
    body = {
        'nome_display':     info['name'],
        'search_metabooks': info['name'],
        'publisher_mb_id':  info['mb_id'] or None,
        'ativo':            False,
        'total_livros_mb':  info['estimated'],
    }
    r = directus('POST', '/items/selos', body)
    if r and r.get('data'):
        print(f'  + {info["name"]:40} ~{info["estimated"]:>6,} livros')
        added += 1
    else:
        print(f'  ! Erro ao adicionar: {info["name"]}', file=sys.stderr)

print(f'\nConcluído: {added} novas editoras adicionadas ao Directus')

# ── 5. Registrar no sync_log ──────────────────────────────────────────────────

now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
log_data = {
    'finalizado_em':  now_str,
    'status':         'concluido',
    'livros_criados': added,
    'progresso_msg':  f'{added} novas editoras descobertas ({total_unique} únicas, catálogo total: {total_catalog:,})',
}

if DISCOVER_LOG_ID:
    directus('PATCH', f'/items/sync_log/{DISCOVER_LOG_ID}', log_data)
    print(f'sync_log {DISCOVER_LOG_ID} atualizado')
else:
    directus('POST', '/items/sync_log', log_data)
