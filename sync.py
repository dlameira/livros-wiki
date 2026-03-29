#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sincroniza livros da Metabooks com o Directus (coleção `biblioteca`).

Variáveis de ambiente necessárias:
  DIRECTUS_URL    https://directus-production-afdd.up.railway.app
  DIRECTUS_TOKEN  token do usuário Claudio
  META_TOKEN      token da API Metabooks
"""

import json
import os
import re
import struct
import sys
if sys.stdout.encoding != 'utf-8':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta

# ── Configuração ──────────────────────────────────────────────────────────────

DIRECTUS_URL   = os.environ.get('DIRECTUS_URL', 'https://directus-production-afdd.up.railway.app')
DIRECTUS_TOKEN = os.environ['DIRECTUS_TOKEN']
META_TOKEN     = os.environ['META_TOKEN']
META_BASE      = 'https://www.metabooks.com/api/v2'
META_COVER_TOKEN = os.environ.get('META_COVER_TOKEN', '')
PAGE_SIZE         = 100
MONTHS_BACK       = int(os.environ.get('MONTHS_BACK', '12'))   # 0 = sem filtro de data
PUBLISHER_FILTER  = os.environ.get('PUBLISHER_FILTER', '')     # ex: "Companhia das Letras"

SYNC_LOG_ID = os.environ.get('SYNC_LOG_ID', '')  # ID do registro em sync_log (opcional)

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_meta(url):
    req = urllib.request.Request(url, headers={'User-Agent': 'livros-wiki/1.0'})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


def directus(method, path, data=None):
    url = f'{DIRECTUS_URL}{path}'
    headers = {
        'Authorization': f'Bearer {DIRECTUS_TOKEN}',
        'Content-Type':  'application/json',
    }
    body = json.dumps(data).encode() if data is not None else None
    req  = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        print(f'  Directus HTTP {e.code}: {e.read().decode()[:300]}', file=sys.stderr)
        return None


def strip_html(s):
    return re.sub(r'<[^>]+>', '', s or '').strip()


def normalize(s):
    s = unicodedata.normalize('NFD', s.lower())
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^a-z0-9 ]', '', s).strip()


def cover_ratio(url):
    """Retorna largura/altura real da imagem lendo só o header (sem baixar tudo)."""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'livros-wiki/1.0'})
        with urllib.request.urlopen(req, timeout=10) as r:
            header = r.read(8192)
        # JPEG: procura marcador SOF (FF C0 ou FF C2)
        i = 0
        while i < len(header) - 8:
            if header[i] == 0xFF and header[i+1] in (0xC0, 0xC1, 0xC2):
                h = struct.unpack('>H', header[i+5:i+7])[0]
                w = struct.unpack('>H', header[i+7:i+9])[0]
                return round(w / h, 3) if h else None
            i += 1
        # PNG: dimensões nos bytes 16-24
        if header[:8] == b'\x89PNG\r\n\x1a\n':
            w = struct.unpack('>I', header[16:20])[0]
            h = struct.unpack('>I', header[20:24])[0]
            return round(w / h, 3) if h else None
    except Exception:
        pass
    return None


def parse_date(s):
    """Converte DD/MM/YYYY ou YYYY-MM-DD... para YYYY-MM-DD."""
    if not s:
        return None
    if '/' in s:
        parts = s.split('/')
        if len(parts) == 3:
            d, m, y = parts
            return f'{y}-{m.zfill(2)}-{d.zfill(2)}'
    return s[:10] if len(s) >= 10 else None


# ── 1. Carregar selos ativos do Directus ──────────────────────────────────────

print('Carregando selos ativos do Directus...')
_selos_resp = directus('GET', '/items/selos?filter[ativo][_eq]=true&fields=nome_display,search_metabooks&limit=200')
if not _selos_resp or not _selos_resp.get('data'):
    print('ERRO: não foi possível carregar selos do Directus', file=sys.stderr)
    sys.exit(1)

PUBLISHERS = [
    {'label': s['nome_display'], 'search': s['search_metabooks']}
    for s in _selos_resp['data']
    if s.get('search_metabooks')
]
print(f'  {len(PUBLISHERS)} selos ativos: {", ".join(p["label"] for p in PUBLISHERS)}\n')

# ── 2. Carregar ISBNs existentes ──────────────────────────────────────────────

print('Carregando ISBNs existentes no Directus...')
existing = {}  # isbn → id
page = 1
while True:
    result = directus('GET', f'/items/biblioteca?fields=id,isbn&limit=500&page={page}')
    if not result or not result.get('data'):
        break
    for item in result['data']:
        if item.get('isbn'):
            existing[item['isbn']] = item['id']
    if len(result['data']) < 500:
        break
    page += 1
print(f'  {len(existing)} livros já existem\n')

# ── 3. Buscar livros na Metabooks ─────────────────────────────────────────────

to_create = []
to_update = []  # lista de (id, payload)

# Data de corte (0 = sem filtro)
# AD= filtra por lastModificationDate (captura novos E editados)
# EJ= filtra só por publicationDate (só novos) — não usado mais
if MONTHS_BACK > 0:
    cutoff = datetime.now(timezone.utc) - timedelta(days=MONTHS_BACK * 30)
    date_from = cutoff.strftime('%Y%m%d')
    date_label = f'modificados desde {cutoff.strftime("%d/%m/%Y")}'
else:
    date_from = None
    date_label = 'catálogo completo'

publishers = [p for p in PUBLISHERS if not PUBLISHER_FILTER or p['label'] == PUBLISHER_FILTER]

# Registra total de editoras para barra de progresso
if SYNC_LOG_ID:
    directus('PATCH', f'/items/sync_log/{SYNC_LOG_ID}', {
        'total_editoras': len(publishers),
        'editoras_processadas': 0,
    })

for pub_idx, pub in enumerate(publishers):
    print(f'Buscando: {pub["label"]} ({date_label})...')
    if SYNC_LOG_ID:
        directus('PATCH', f'/items/sync_log/{SYNC_LOG_ID}', {
            'editoras_processadas': pub_idx,
            'progresso_msg': f'Buscando {pub["label"]}...',
        })
    try:
        base_query = f'VL={pub["search"]}'
        if date_from:
            base_query += f' AND AD={date_from}^99991231'
        search_query = urllib.parse.quote(base_query)
        page = 0
        count = 0

        while True:
            url = (f'{META_BASE}/products?access_token={META_TOKEN}'
                   f'&search={search_query}&size={PAGE_SIZE}'
                   f'&sort=publicationDate&direction=desc&page={page}')
            j = fetch_meta(url)
            items = j.get('content', [])
            if not items:
                break

            for b in items:
                if b.get('productType') != 'pbook':
                    continue

                isbn = (b.get('gtin') or b.get('isbn') or '').replace('-', '').replace(' ', '')
                if not isbn:
                    continue

                pub_date = parse_date(b.get('publicationDate') or b.get('onSaleDate'))

                contributors = [
                    c for c in (b.get('contributors') or [])
                    if c.get('firstName') or c.get('lastName') or c.get('groupName')
                ]

                capa_url = (f"{b['coverUrl']}?access_token={META_COVER_TOKEN}" if b.get('coverUrl') and META_COVER_TOKEN else b.get('coverUrl') or '')

                payload = {
                    'isbn':            isbn,
                    'titulo':          (b.get('title') or '—')[:500],
                    'autor':           (b.get('author') or '')[:255],
                    'editora':         (b.get('publisher') or pub['label'])[:255],
                    'capa_url':        capa_url,
                    'sinopse':         strip_html(b.get('mainDescription') or b.get('shortDescription')),
                    'biografia_autor': strip_html(b.get('biographicalNote')),
                    'contributors':    contributors or None,
                    'data_publicacao': pub_date,
                }

                if isbn in existing:
                    to_update.append((existing[isbn], payload))
                else:
                    to_create.append(payload)

                count += 1

            if j.get('last', True):
                break
            page += 1

        print(f'  {count} livros encontrados')
        if SYNC_LOG_ID:
            directus('PATCH', f'/items/sync_log/{SYNC_LOG_ID}', {
                'editoras_processadas': pub_idx + 1,
                'progresso_msg': f'{pub["label"]}: {count} livros',
            })

    except Exception as e:
        print(f'  ERRO: {e}', file=sys.stderr)

# ── 4. Deduplicar por ISBN ────────────────────────────────────────────────────

seen = {}
for p in to_create:
    seen[p['isbn']] = p
to_create = list(seen.values())

seen_upd = {}
for id_, p in to_update:
    seen_upd[p['isbn']] = (id_, p)
to_update = list(seen_upd.values())

# ── 5. Criar novos ────────────────────────────────────────────────────────────

print(f'\nCriando {len(to_create)} novos livros...')
if to_create:
    BATCH = 100
    for i in range(0, len(to_create), BATCH):
        result = directus('POST', '/items/biblioteca', to_create[i:i+BATCH])
        if result:
            print(f'  Lote {i//BATCH+1}: {len(result.get("data", []))} criados')

# ── 6. Atualizar existentes ───────────────────────────────────────────────────

print(f'Atualizando {len(to_update)} livros existentes...')
if to_update:
    BATCH = 50
    for i in range(0, len(to_update), BATCH):
        batch   = to_update[i:i+BATCH]
        payload = [{'id': id_, **data} for id_, data in batch]
        result  = directus('PATCH', '/items/biblioteca', payload)
        if result:
            print(f'  Lote {i//BATCH+1}: {len(result.get("data", []))} atualizados')

now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
print(f'\nConcluído em {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")} UTC')

# ── 7. Registrar resultado no sync_log ────────────────────────────────────────

if SYNC_LOG_ID:
    directus('PATCH', f'/items/sync_log/{SYNC_LOG_ID}', {
        'finalizado_em':    now_str,
        'status':           'concluido',
        'livros_criados':   len(to_create),
        'livros_atualizados': len(to_update),
    })
    print(f'sync_log {SYNC_LOG_ID} atualizado')
else:
    editoras_list = [p['label'] for p in PUBLISHERS]
    directus('POST', '/items/sync_log', {
        'finalizado_em':    now_str,
        'status':           'concluido',
        'editoras':         editoras_list,
        'months_back':      MONTHS_BACK,
        'livros_criados':   len(to_create),
        'livros_atualizados': len(to_update),
    })
