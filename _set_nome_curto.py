#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Calcula nome_curto para todos os selos, removendo prefixos/sufixos
"Editora", "Edições" e "Editorial" do nome_display.
O nome_display original é mantido para indexação na API.
"""

import json
import os
import re
import sys
if sys.stdout.encoding != 'utf-8':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)

import urllib.request
import urllib.error

DIRECTUS = os.environ.get('DIRECTUS_URL', 'https://directus-production-afdd.up.railway.app')
TOKEN    = os.environ.get('DIRECTUS_TOKEN', '73IhvvuNMBy-et-_0Ore5ZH2jo25pnU5')

def req(method, path, body=None):
    url = DIRECTUS + path
    data = json.dumps(body).encode() if body else None
    r = urllib.request.Request(url, data=data, method=method,
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(r, timeout=20) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f'  HTTP {e.code}: {e.read().decode()[:200]}', file=sys.stderr)
        return None

# Palavras a remover do início e do fim
_PREFIXOS = re.compile(
    r'^(editora|edi[çc][oõ]es|editorial)\s+',
    re.IGNORECASE
)
_SUFIXOS = re.compile(
    r'\s+(editora|editorial)$',
    re.IGNORECASE
)

def compute_nome_curto(nome_display):
    if not nome_display:
        return None
    s = nome_display.strip()
    s = _PREFIXOS.sub('', s)
    s = _SUFIXOS.sub('', s)
    s = s.strip()
    # Se ficou vazio ou igual ao original, não precisa salvar
    if not s or s.lower() == nome_display.lower():
        return None
    return s

# Buscar todos os selos
print('Carregando selos...')
page, selos = 1, []
while True:
    r = req('GET', f'/items/selos?fields=id,nome_display,nome_curto&limit=500&page={page}')
    data = (r or {}).get('data', [])
    if not data:
        break
    selos.extend(data)
    if len(data) < 500:
        break
    page += 1
print(f'  {len(selos)} selos carregados')

# Calcular e aplicar
atualizados, inalterados = 0, 0
preview = []

for s in selos:
    nome_curto = compute_nome_curto(s['nome_display'])
    if nome_curto is None:
        inalterados += 1
        continue
    r = req('PATCH', f'/items/selos/{s["id"]}', {'nome_curto': nome_curto})
    if r:
        atualizados += 1
        preview.append(f'  {s["nome_display"]:40} → {nome_curto}')

print(f'\n{atualizados} selos atualizados, {inalterados} sem alteração\n')
print('Amostras:')
for line in preview[:30]:
    print(line)
if len(preview) > 30:
    print(f'  ... e mais {len(preview)-30}')
