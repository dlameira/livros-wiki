import urllib.request, json, os, sys
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)

DIRECTUS = 'https://directus-production-afdd.up.railway.app'
TOKEN = '73IhvvuNMBy-et-_0Ore5ZH2jo25pnU5'

def req(method, path, body=None):
    url = DIRECTUS + path
    data = json.dumps(body).encode() if body else None
    r = urllib.request.Request(url, data=data, method=method,
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'})
    with urllib.request.urlopen(r, timeout=15) as resp:
        return json.loads(resp.read().decode())

grupos_def = [
    {'nome': 'Grupo Schwarcz', 'cor': '#1a6b3a'},
    {'nome': 'Grupo Record',   'cor': '#8b1a1a'},
    {'nome': 'Grupo Rocco',    'cor': '#1a3f8b'},
    {'nome': 'GMT Editores',   'cor': '#7b4a1a'},
    {'nome': 'Independente',   'cor': '#4a4a4a'},
]

grupo_ids = {}
for g in grupos_def:
    r = req('POST', '/items/grupos_editoriais', g)
    grupo_ids[g['nome']] = r['data']['id']
    print(f'  grupo: {g["nome"]} → id {r["data"]["id"]}')

S = grupo_ids['Grupo Schwarcz']
R = grupo_ids['Grupo Record']
RO = grupo_ids['Grupo Rocco']
GMT = grupo_ids['GMT Editores']
IND = grupo_ids['Independente']

selos = [
    # Grupo Schwarcz
    ('Companhia das Letras',   'Companhia das Letras',   'BR0089554', S,   True),
    ('Zahar',                  'Zahar',                  'BR0089671', S,   False),
    ('Objetiva',               'Objetiva',               'BR0089588', S,   False),
    ('Alfaguara',              'Alfaguara',              'BR0089587', S,   False),
    ('Companhia das Letrinhas','Companhia das Letrinhas', 'BR0089555', S,   False),
    ('Paralela',               'Paralela',               'BR0089560', S,   False),
    ('Seguinte',               'Seguinte',               'BR0089576', S,   False),
    ('Quadrinhos na Cia',      'Quadrinhos na Cia',      'BR0089564', S,   False),
    ('Companhia de Bolso',     'Companhia de Bolso',     'BR0089558', S,   False),
    ('Fontanar',               'Fontanar',               'BR0089589', S,   False),
    # Grupo Record
    ('Record',                 'Record',                 'BR0089628', R,   True),
    ('Galera',                 'Galera',                 'BR0089629', R,   False),
    ('BestSeller',             'BestSeller',             'BR0089633', R,   False),
    # Grupo Rocco
    ('Rocco',                  'Rocco',                  'BR0089959', RO,  True),
    ('Rocco Jovens Leitores',  'Rocco Jovens Leitores',  'BR0089965', RO,  False),
    # GMT
    ('Sextante',               'Sextante',               'BR0089569', GMT, True),
    # Independentes
    ('Intrínseca',             'Intrinseca',             'BR0089539', IND, True),
    ('Darkside',               'Darkside',               None,        IND, True),
    ('Autêntica',              'Autentica',              'BR0089524', IND, True),
    ('Arqueiro',               'Arqueiro',               None,        IND, True),
    ('Todavia',                'Todavia',                None,        IND, True),
    ('Fósforo',                'Fosforo Editora',        None,        IND, True),
    ('Aleph',                  'Aleph',                  None,        IND, True),
    ('Antofágica',             'Antofagica',             None,        IND, True),
    ('Ubu',                    'Ubu Editora',            None,        IND, True),
    ('Bazar do Tempo',         'Bazar do Tempo',         None,        IND, True),
    ('Editora 34',             'Editora 34',             None,        IND, True),
    ('Seiva',                  'Seiva',                  'BR0204988', IND, True),
]

for nome_display, search, mb_id, grupo, ativo in selos:
    body = {'nome_display': nome_display, 'search_metabooks': search,
            'publisher_mb_id': mb_id, 'grupo': grupo, 'curada': ativo}
    req('POST', '/items/selos', body)
    print(f'  selo: {nome_display:30} ativo={ativo}')

print('Concluído!')
