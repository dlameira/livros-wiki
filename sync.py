#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sincroniza livros da Metabooks com o Directus (coleção `biblioteca`).

Deploy no Railway como Cron Job:
  Comando: python sync.py
  Schedule: 0 */6 * * *  (a cada 6 horas)

Variáveis de ambiente necessárias:
  DIRECTUS_URL    https://directus-production-afdd.up.railway.app
  DIRECTUS_TOKEN  token do usuário Claudio
  META_TOKEN      token da API Metabooks
"""

import json
import os
import re
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
PAGE_SIZE      = 40
MONTHS_BACK    = 6  # janela de publicação para sincronizar

PUBLISHERS = [
    {'label': 'Companhia das Letras', 'search': 'Companhia das Letras'},
    {'label': 'Intrínseca',           'search': 'Intrinseca'},
    {'label': 'Seiva',                'search': 'Seiva'},
    {'label': 'Rocco',                'search': 'Rocco'},
    {'label': 'Todavia',              'search': 'Todavia'},
    {'label': 'Fósforo',              'search': 'Fosforo Editora'},
    {'label': 'Darkside',             'search': 'Darkside'},
    {'label': 'Autêntica',            'search': 'Autentica'},
    {'label': 'Arqueiro',             'search': 'Arqueiro'},
    {'label': 'Record',               'search': 'Record'},
]

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


# ── 1. Carregar ISBNs existentes ──────────────────────────────────────────────

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

# ── 2. Buscar livros na Metabooks ─────────────────────────────────────────────

to_create = []
to_update = []  # lista de (id, payload)

# Data de corte: MONTHS_BACK meses atrás, formato YYYYMMDD para a API Metabooks
cutoff = datetime.now(timezone.utc) - timedelta(days=MONTHS_BACK * 30)
date_from = cutoff.strftime('%Y%m%d')

for pub in PUBLISHERS:
    print(f'Buscando: {pub["label"]} (publicações desde {cutoff.strftime("%d/%m/%Y")})...')
    try:
        search_query = urllib.parse.quote(f'VL={pub["search"]} AND EJ={date_from}^99991231')
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
                isbn = (b.get('gtin') or b.get('isbn') or '').replace('-', '').replace(' ', '')
                if not isbn:
                    continue

                pub_date = parse_date(b.get('publicationDate') or b.get('onSaleDate'))

                payload = {
                    'isbn':            isbn,
                    'titulo':          (b.get('title') or '—')[:500],
                    'autor':           (b.get('author') or '')[:255],
                    'editora':         (b.get('publisher') or pub['label'])[:255],
                    'capa_url':        b.get('coverUrl') or '',
                    'sinopse':         strip_html(b.get('mainDescription') or b.get('shortDescription')),
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

    except Exception as e:
        print(f'  ERRO: {e}', file=sys.stderr)

# ── 3. Deduplicar por ISBN ────────────────────────────────────────────────────

seen = {}
for p in to_create:
    seen[p['isbn']] = p
to_create = list(seen.values())

seen_upd = {}
for id_, p in to_update:
    seen_upd[p['isbn']] = (id_, p)
to_update = list(seen_upd.values())

# ── 4. Criar novos ────────────────────────────────────────────────────────────

print(f'\nCriando {len(to_create)} novos livros...')
if to_create:
    BATCH = 100
    for i in range(0, len(to_create), BATCH):
        result = directus('POST', '/items/biblioteca', to_create[i:i+BATCH])
        if result:
            print(f'  Lote {i//BATCH+1}: {len(result.get("data", []))} criados')

# ── 5. Atualizar existentes ───────────────────────────────────────────────────

print(f'Atualizando {len(to_update)} livros existentes...')
if to_update:
    BATCH = 50
    for i in range(0, len(to_update), BATCH):
        batch   = to_update[i:i+BATCH]
        payload = [{'id': id_, **data} for id_, data in batch]
        result  = directus('PATCH', '/items/biblioteca', payload)
        if result:
            print(f'  Lote {i//BATCH+1}: {len(result.get("data", []))} atualizados')

print(f'\nConcluído em {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")} UTC')
