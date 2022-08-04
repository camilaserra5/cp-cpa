from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import requests
import re
import pandas as pd
import csv
import difflib
import time

pd.options.display.max_colwidth = 1000
pd.options.mode.chained_assignment = None

start_time = time.time()

#### Leo CSV con la base de datos de códigos postales
df = pd.read_csv('df-backup2.csv')
df = df.drop('index', axis=1)
df = df.drop('Unnamed: 0', axis=1)
print ('Cantidad de registros en el csv de CPAs: ' + str(df.shape))

#### limpio los datos
def removeURL(x, num):
    prov = x.split('/')
    if (len(prov)==1):
        return prov[0]
    return prov[num]

df['provincia'] = df['provincia'].apply(lambda x: removeURL(x, 4))
df['localidad'] = df['localidad'].apply(lambda x: removeURL(x, 5))
df['localidad'] = df['localidad'].str.replace('-', ' ')
df['localidad'] = df['localidad'].str.lower()
df['provincia'] = df['provincia'].str.lower()
df['calle'] = df['calle'].str.lower()
df = df[(df['provincia'] == 'cordoba') | (df['provincia'] == 'córdoba')]
print ('Cantidad de registros en el csv de CPAs: ' + str(df.shape))

### Leo CSV con los users de lemon
df_lemon = pd.read_csv('lemoncash_ar_CORDOBA.csv')
df_lemon = df_lemon[df_lemon['zip_code'].isin(df_lemon['zip_code'].value_counts().index.tolist()[:40])]
df_lemon.columns = ['calle', 'num', 'localidad', 'prov', 'cp']
df_lemon = df_lemon[df_lemon['calle'].notnull()]
df_lemon['calle'] = df_lemon['calle'].str.lower()
df_lemon['prov'] = df_lemon['prov'].str.lower()
df_lemon['localidad'] = df_lemon['localidad'].str.lower()
df_lemon['num'] = df_lemon['num'].fillna(0).astype(int)
df_lemon['par'] = (df_lemon['num'] % 2) == 0
df_lemon['cp'] = pd.to_numeric(df_lemon['cp'])
print ('Cantidad de registros en el csv de LEMON: ' + str(df_lemon.shape))

print("--- Normalizando los datos.... ---")

#### normalizo todo para sacar tildes y ñ
def normalize(s):
    replacements = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
        ("ñ", "n"),
        ("ü", "u")
    )
    for a, b in replacements:
        s = s.replace(a, b).replace(a.upper(), b.upper())
    return s

df_lemon['calle'] = df_lemon['calle'].apply(lambda x: normalize(str(x)))
df_lemon['prov'] = df_lemon['prov'].apply(lambda x: normalize(str(x)))
df_lemon['localidad'] = df_lemon['localidad'].apply(lambda x: normalize(str(x)))

df['provincia'] = df['provincia'].apply(lambda x: normalize(str(x)))
df['localidad'] = df['localidad'].apply(lambda x: normalize(str(x)))
df['calle'] = df['calle'].apply(lambda x: normalize(str(x)))

#### Busco la letra de la prov (primer caracter del CPA)
def province_letter(y):
    x = y['prov']
    if x == 'salta':
        return 'A'
    elif x == 'provincia de buenos aires':
        return 'B'
    elif x == 'buenos aires':
        return 'C'
    elif x == 'san luis':
        return 'D'
    elif x == 'entre rios':
        return 'E'
    elif x == 'la rioja':
        return 'F'
    elif x == 'santiago del estero':
        return 'G'
    elif x == 'chaco':
        return 'H'
    elif x == 'san juan':
        return 'J'
    elif x == 'catamarca':
        return 'K'
    elif x == 'la pampa':
        return 'L'
    elif x == 'mendoza':
        return 'M'
    elif x == 'misiones':
        return 'N'
    elif x == 'formosa':
        return 'P'
    elif x == 'neuquen':
        return 'Q'
    elif x == 'rio negro':
        return 'R'
    elif x == 'santa fe':
        return 'S'
    elif x == 'tucuman':
        return 'T'
    elif x == 'chubut':
        return 'U'
    elif x == 'tierra del fuego':
        return 'V'
    elif x == 'corrientes':
        return 'W'
    elif x == 'cordoba':
        return 'X'
    elif x == 'jujuy':
        return 'Y'
    elif x == 'santa cruz':
        return 'Z'
    return ''

df_lemon['letra'] = df_lemon.apply(lambda x: province_letter(x), axis=1)

## busco cp dentro del cpa por las dudas
df['alt-cp'] = df['cpa'].str[1:5]
df['alt-cp'] = pd.to_numeric(df['alt-cp'])
## filtro los que no tengo una prov conocida (brasil)
df_lemon = df_lemon[df_lemon['letra'] != '']
print ('Cantidad reg LEMON post normalizar: ' + str(df_lemon.shape))
print("La normalización de datos tardó: %s segundos ---" % (time.time() - start_time))



## Busco si pertenece a una localidad con CPA único
def cpa_unico(x):
    df_new = df_cpa_unico[df_cpa_unico['localidad'] == x['localidad']]
    df_new = df_new[df_new['provincia'] == x['prov']]
    if (len(df_new)==1):
        return df_new['cpa'].tolist()
    return ''


df_cpa_unico = df[df['calle']=='nan']
df_lemon['cpa'] = df_lemon.apply(lambda x: cpa_unico(x), axis=1)
print('Encontré ' + str(df_lemon[df_lemon['cpa'].apply(lambda x: len(x)!=0)].shape[0]) + ' con CPA de localidad unicos! ')

### Funcion para buscar la mejor coincidencia de nombre de calle
### Dentro de la provincia y mismo CP, busco la calle más similar con umbral de 60, si no devuelvo string vacio
def closest_match_fuzzywuzzy(x):
    try:
        if (len(x['cpa']) != 0):
            return ''
        df_new = df.loc[x['letra']==df['cpa'].str[0:1]]
        df_new = df_new.loc[(df_new['cp'] == x['cp']) | (df_new['alt-cp'] == x['cp'])]
        bestmatch = process.extract(str(x['calle']), df_new['calle'], scorer=fuzz.token_set_ratio, limit=1)
        if len(bestmatch)>0 and bestmatch[0][1]>60:
            return bestmatch[0][0]
        return ''
    except:
        return ''


print("--- Buscando calles similares.... ---")
df_lemon['bestmatch_fuzzywuzzy'] = df_lemon.apply(lambda x: closest_match_fuzzywuzzy(x), axis=1)
print('Ctd registros con calle similar encontrada: ' + str(df_lemon[df_lemon['bestmatch_fuzzywuzzy']!=''].shape[0]))
print('Ctd registros sin calle similar encontrada: ' + str(df_lemon[df_lemon['bestmatch_fuzzywuzzy']==''].shape[0]))
print("La búsqueda de calles similares tardó: %s segundos ---" % (time.time() - start_time))

### Funcion para buscar el CPA
### Busca por la calle con numero y si es o no par.
def find_CPA(x):
    if x['bestmatch_fuzzywuzzy'] == '':
        return ''
    if (len(x['cpa']) != 0):
        return x['cpa']
    ## Busco dentro del mismo cp
    df_new = df[df['cp'] == x['cp']]
    df_new = df_new[df_new['calle'] == x['bestmatch_fuzzywuzzy']]
    df_new = df_new[(df_new["desde"]<=x['num']) & (df_new["hasta"]>=x['num'])]
    df_new = df_new[df_new['par']==x['par']]
    if len(df_new) != 0:
        return df_new['cpa'].tolist()

    ## Busco dentro del mismo cpa
    df_new = df[df['cpa'].str[0:1] == x['cp']]
    df_new = df_new[df_new['calle'] == x['bestmatch_fuzzywuzzy']]
    df_new = df_new[(df_new["desde"]<=x['num']) & (df_new["hasta"]>=x['num'])]
    df_new = df_new[df_new['par']==x['par']]
    if len(df_new) != 0:
        return df_new['cpa'].tolist()

    ## Busco dentro de la misma letra
    df_new = df[df['cpa'].str[0:1] == x['letra']]
    df_new = df_new[df_new['calle'] == x['bestmatch_fuzzywuzzy']]
    df_new = df_new[(df_new["desde"]<=x['num']) & (df_new["hasta"]>=x['num'])]
    df_new = df_new[df_new['par']==x['par']]

    return df_new['cpa'].tolist()

def to_unique_list(x):
    return list(dict.fromkeys(x['cpa']))

## Busco el CPA
print("--- Buscando CPAs.... ---")
df_lemon['cpa'] = df_lemon.apply(lambda x: find_CPA(x), axis=1)
df_lemon['cpa'] = df_lemon.apply(lambda x: to_unique_list(x), axis=1)
print('Encontré ' + str(df_lemon[df_lemon['cpa'].apply(lambda x: len(x) == 1)].shape[0]) + ' CPAs!!')


### Cuantos registros tengo con calle pero sin CPA
df_withou_cpa = df_lemon[df_lemon['cpa'].apply(lambda x: len(x) != 1)]
df_withou_cpa = df_withou_cpa[df_withou_cpa['bestmatch_fuzzywuzzy']!='']
print('Tengo ' + str(df_withou_cpa.shape[0]) + ' registros sin CPA pero con calle...')
print(' tienen num ' + str(df_withou_cpa[df_withou_cpa['num']!=0].shape[0]))
print(' no tienen num ' + str(df_withou_cpa[df_withou_cpa['num'] == 0].shape[0]))


## Si no tiene num -> le asigno el 1ero
## Si tiene num -> busco el más cercano
def get_cpa(x):
    if len(x['cpa'])==1: # si ya encontré antes no hago nada.
        return ''
    if x['bestmatch_fuzzywuzzy'] == '':
        return ''

    df_new = df[(df['cp'] == x['cp']) | (df['alt-cp'] == x['cp'])]
    df_new = df_new[df_new['calle'] == x['bestmatch_fuzzywuzzy']]

    if x['num'] == 0:
        return df_new[0:1]['cpa']
    else:
        dist = (df_new['desde'] - x['num']).abs()
        return [df_new.loc[dist.idxmin()]['cpa']]



def to_unique_list(x):
    return list(dict.fromkeys(x['cpa_v2']))

print("--- Buscando CPAs.... ---")
df_lemon['cpa_v2'] = df_lemon.apply(lambda x: get_cpa(x), axis=1)
df_lemon['cpa_v2'] = df_lemon.apply(lambda x: to_unique_list(x), axis=1)
print('Encontré ' + str(df_lemon[df_lemon['cpa_v2'].apply(lambda x: len(x) == 1)].shape[0]) + ' CPAs!!')




df_withou_cpa = df_lemon[df_lemon['cpa'].apply(lambda x: len(x) != 1)]
df_withou_cpa = df_withou_cpa[df_withou_cpa['cpa_v2'].apply(lambda x: len(x) != 1)]
print('total sin CPA aún  --> ' + str(df_withou_cpa.shape[0]))
print('  tienen num ' + str(df_withou_cpa[df_withou_cpa['num']!=0].shape[0]))
print('  no tienen num ' + str(df_withou_cpa[df_withou_cpa['num'] == 0].shape[0]))


## Si no tiene num -> le asigno el 1ero
## Si tiene num -> busco el más cercano
def get_cpa(x):
    if len(x['cpa'])==1: # si ya encontré antes no hago nada.
        return ''
    if len(x['cpa_v2'])==1: # si ya encontré antes no hago nada.
        return ''

    if x['num'] == 0:
        df_new = df[(df['cp'] == x['cp']) | (df['alt-cp'] == x['cp'])]
        return df_new[0:1]['cpa']
    else:
        df_new = df[(df['cp'] == x['cp']) | (df['alt-cp'] == x['cp'])]
        dist = (df_new['desde'] - x['num']).abs()
        return [df_new.loc[dist.idxmin()]['cpa']]

def to_unique_list(x):
    return list(dict.fromkeys(x['cpa_v3']))


print('--- Buscando CPAs dentro de todo el conjunto...')
df_lemon['cpa_v3'] = df_lemon.apply(lambda x: get_cpa(x), axis=1)
df_lemon['cpa_v3'] = df_lemon.apply(lambda x: to_unique_list(x), axis=1)
print('Encontré ' + str(df_lemon[df_lemon['cpa_v3'].apply(lambda x: len(x) == 1)].shape[0]) + ' CPAs!! :) ')
print("Ultima Busqueda CPAS tardó %s segundos ---" % (time.time() - start_time))


## 2d0 casos que aun no tengo nombre de calle similar
df_withou_cpa = df_lemon[df_lemon['cpa'].apply(lambda x: len(x) != 1)]
df_withou_cpa = df_withou_cpa[df_withou_cpa['cpa_v2'].apply(lambda x: len(x) != 1)]
df_withou_cpa = df_withou_cpa[df_withou_cpa['cpa_v3'].apply(lambda x: len(x) != 1)]
print('Total sin CPA :(   --->' + str(df_withou_cpa.shape[0]))


def CPA_FINAL(x):
    if len(x['cpa'])==1:
        return x['cpa'][0]
    if len(x['cpa_v2'])==1:
        return x['cpa_v2'][0]
    if len(x['cpa_v3'])==1:
        return x['cpa_v3'][0]

    return ''

df_lemon['cpaFINAL'] = df_lemon.apply(lambda x: CPA_FINAL(x), axis=1)

### Exporto a csv
print("--- TIEMPO TOTAL: %s segundos ---" % (time.time() - start_time))
df_lemon.to_csv('resultados-cpas-ultimo-ultimo_CAMI.csv', sep=',')
