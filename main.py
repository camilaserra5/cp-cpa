from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import re

pd.options.display.max_colwidth = 1000
pd.options.mode.chained_assignment = None

print('*** Start processing data ***')
df = pd.read_csv('argentina-CPAS.csv')
print('Number of rows in general csv: ' + str(df.shape))

# Leo CSV con los users de lemon
df_lemon = pd.read_csv('lemoncash_ar_muestra.csv')
df_lemon.columns = ['calle', 'num', 'localidad', 'prov', 'cp']
df_lemon = df_lemon[df_lemon['calle'].notnull()]
df_lemon['calle'] = df_lemon['calle'].str.lower()
df_lemon['prov'] = df_lemon['prov'].str.lower()
df_lemon['localidad'] = df_lemon['localidad'].str.lower()
df_lemon['num'] = df_lemon['num'].fillna(0).astype(int)
df_lemon['par'] = (df_lemon['num'] % 2) == 0

print('Number of rows in lemon csv: ' + str(df_lemon.shape))


def numeric_cp(s):
    match = re.search('\\d{4}', s)
    if match:
        return int(match.group())
    return 0


df_lemon['cp'] = df_lemon['cp'].apply(lambda x: numeric_cp(str(x)))


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

df['calle'] = df['calle'].apply(lambda x: normalize(str(x)))
df['provincia'] = df['provincia'].apply(lambda x: normalize(str(x)))
df['localidad'] = df['localidad'].apply(lambda x: normalize(str(x)))


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

print('Number of rows with unknown province: ' + str(df_lemon[df_lemon['letra'] == ''].shape))
print('Number of rows with non-numeric cpa: ' + str(df_lemon[df_lemon['cp'] == 0].shape))


def unprocessable_row(x):
    if x['letra'] == '':
        return list('X')
    if x['cp'] == 0:
        return list('X')
    return ''


df_lemon['cpa'] = df_lemon.apply(lambda x: unprocessable_row(x), axis=1)
print('Number of processable rows: ' + str(df_lemon[df_lemon['cpa'] == ''].shape))

print('*** Starting CPA search ***')


def has_valid_cpa(x):
    if x == list('X'):
        return False
    return len(x) == 1


# Searching if the combination of localidad-provincia has a unique CPA
def find_unique_cpa(x):
    if len(x['cpa']) == 1:
        return x['cpa']
    df_new = df_cpa_unico[df_cpa_unico['localidad'] == x['localidad']]
    df_new = df_new[df_new['provincia'] == x['prov']]
    if len(df_new) == 1:
        return df_new['cpa'].tolist()
    return ''


df_cpa_unico = df[df['calle'] == 'nan']
df_lemon['cpa'] = df_lemon.apply(lambda x: find_unique_cpa(x), axis=1)
size = df_lemon[df_lemon['cpa'].apply(lambda x: has_valid_cpa(x))].shape[0]
print('Found ' + str(size) + ' CPAs for unique localidad-provincia ')


# Searching the best street name match inside the province and postal code
def closest_match_fuzzywuzzy(x):
    try:
        if len(x['cpa']) == 1:
            return ''
        df_new = df.loc[x['letra'] == df['cpa'].str[0:1]]
        df_new = df_new.loc[(df_new['cp'] == x['cp']) | (df_new['alt-cp'] == x['cp'])]
        bestmatch = process.extract(str(x['calle']), df_new['calle'], scorer=fuzz.token_set_ratio, limit=1)
        if len(bestmatch) > 0 and bestmatch[0][1] > 60:
            return bestmatch[0][0]
        return ''
    except:
        return ''


df_lemon['bestmatch_fuzzywuzzy'] = df_lemon.apply(lambda x: closest_match_fuzzywuzzy(x), axis=1)
print('Number of rows with similar street name found: ' + str(df_lemon[df_lemon['bestmatch_fuzzywuzzy'] != ''].shape[0]))
print('Number of rows without similar street name found: ' + str(df_lemon[df_lemon['bestmatch_fuzzywuzzy'] == ''].shape[0]))


# Searching CPA by name, number and even/odd
def get_cpa_iteration1(x):
    if len(x['cpa']) == 1:
        return x['cpa']
    if x['bestmatch_fuzzywuzzy'] == '':
        return ''

    df_new = df[df['calle'] == x['bestmatch_fuzzywuzzy']]
    df_new = df_new[(df_new["desde"] <= x['num']) & (df_new["hasta"] >= x['num'])]
    df_new = df_new[df_new['par'] == x['par']]

    # Inside same zip code
    df_new1 = df_new[df_new['cp'] == x['cp']]
    if len(df_new1) != 0:
        return df_new1['cpa'].tolist()

    # Inside same alternative zip code (new zip code)
    df_new2 = df_new[df_new['alt-cp'] == x['cp']]
    return df_new2['cpa'].tolist()


def to_unique_list(x):
    return list(dict.fromkeys(x['cpa']))


df_lemon['cpa'] = df_lemon.apply(lambda x: get_cpa_iteration1(x), axis=1)
df_lemon['cpa'] = df_lemon.apply(lambda x: to_unique_list(x), axis=1)
size_iter1 = df_lemon[df_lemon['cpa'].apply(lambda x: has_valid_cpa(x))].shape[0]
print('Found ' + str(size_iter1) + ' CPAs in iteration 1')

df_without_cpa = df_lemon[df_lemon['cpa'].apply(lambda x: len(x) != 1)]
df_without_cpa = df_without_cpa[df_without_cpa['bestmatch_fuzzywuzzy'] != '']
print('Number of rows without CPA but with street name: ' + str(df_without_cpa.shape[0]))
print('With street number: ' + str(df_without_cpa[df_without_cpa['num'] != 0].shape[0]))
print('Without street number:' + str(df_without_cpa[df_without_cpa['num'] == 0].shape[0]))


# Get CPA with broader params. If it has street number, find closest. If it does not have street number, find first.
def get_cpa_iteration2(x):
    if len(x['cpa']) == 1:
        return x['cpa']
    if x['bestmatch_fuzzywuzzy'] == '':
        return ''

    df_new = df[(df['cp'] == x['cp']) | (df['alt-cp'] == x['cp'])]
    df_new = df_new[df_new['calle'] == x['bestmatch_fuzzywuzzy']]
    if x['num'] == 0:
        return df_new[0:1]['cpa']
    else:
        dist = (df_new['desde'] - x['num']).abs()
        return [df_new.loc[dist.idxmin()]['cpa']]


df_lemon['cpa'] = df_lemon.apply(lambda x: get_cpa_iteration2(x), axis=1)
df_lemon['cpa'] = df_lemon.apply(lambda x: to_unique_list(x), axis=1)
size_iter2 = df_lemon[df_lemon['cpa'].apply(lambda x: has_valid_cpa(x))].shape[0]
print('Found ' + str(size_iter2 - size_iter1) + ' CPAs in iteration 2')

df_without_cpa = df_lemon[df_lemon['cpa'].apply(lambda x: len(x) != 1)]
print('Number of rows without CPA: ' + str(df_without_cpa.shape[0]))
print('With street number: ' + str(df_without_cpa[df_without_cpa['num'] != 0].shape[0]))
print('Without street number:' + str(df_without_cpa[df_without_cpa['num'] == 0].shape[0]))


# Last iteration to find random CPA inside the zip code.
def get_generic_cpa(x):
    if len(x['cpa']) == 1:
        return x['cpa']

    df_new = df[(df['cp'] == x['cp']) | (df['alt-cp'] == x['cp'])]
    if x['num'] == 0:
        return df_new[0:1]['cpa']
    else:
        dist = (df_new['desde'] - x['num']).abs()
        return [df_new.loc[dist.idxmin()]['cpa']]


df_lemon['cpa'] = df_lemon.apply(lambda x: get_generic_cpa(x), axis=1)
df_lemon['cpa'] = df_lemon.apply(lambda x: to_unique_list(x), axis=1)
size_iter3 = df_lemon[df_lemon['cpa'].apply(lambda x: has_valid_cpa(x))].shape[0]
print('Found ' + str(size_iter3 - size_iter2) + ' CPAs in iteration 3')

df_without_cpa = df_lemon[df_lemon['cpa'].apply(lambda x: len(x) != 1)]
print('Number of rows without CPA: ' + str(df_without_cpa.shape[0]))


def final_cpa(x):
    if len(x['cpa']) == 1:
        return x['cpa'][0]
    return 'X'


print('*** Exporting results to csv ***')
df_lemon['final_cpa'] = df_lemon.apply(lambda x: final_cpa(x), axis=1)
df_lemon.to_csv('resultados-cpas.csv', sep=',', index=False)
