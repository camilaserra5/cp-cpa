# Python3
from bs4 import BeautifulSoup
import requests
import re
import csv



## NEW
def unicoCPAEnLocalidad(soup):
  div = soup.find('div', { 'class' : 'question' })
  result = re.search('Sólo existe un CPA para (.*) en provincia (.*) y es (.*)', div.getText())
  if (result != None):
    return {'cpa': result.group(3), 'calle':'', 'par':'', 'min':'', 'max':'', 'localidad':result.group(1), 'provincia':result.group(2)}

def masDeUnCPAEnLocalidad(soup):
  urls = set()
  div = soup.find('div', { 'class' : 'question' })
  link = div.select('a[href^="https://codigo-postal.co/argentina/"]')
  for a in link:
    urls.add(a['href'])
  return urls

def findInTable(url):
  soup = BeautifulSoup(requests.get(url).text, "html.parser")
  result = re.search('https://codigo-postal.co/argentina/(.*)/(.*)/(.*)/', url)
  if result.group(3) == '':
    return []
  table = soup.find('table', { 'class' : 'table table-responsive table-striped table-condensed table-hover' })
  if (table == None):
    texto = soup.find('p').getText()
    result = re.search('(.*) de (.*), (.*) tiene el CPA (.*) válido', texto)
    if result.group(1) == None:
      print('URL' + url)
      return []
    calle= result.group(1)
    localidad= result.group(2)
    provincia= result.group(3)
    cpa = result.group(4)
    return [[calle, '0', '0', '0' , '0', cpa, localidad, provincia]]
  else:
    result = re.search('https://codigo-postal.co/argentina/(.*)/(.*)/(.*)/', url)
    local = result.group(1)
    prov = result.group(2)
    table_body = table.find('tbody')
    data = []
    rows = table_body.find_all('tr')
    for row in rows:
      cols = row.find_all('td')
      cols = [ele.text.strip() for ele in cols]
      data.append([ele for ele in cols if ele] + [local] + [prov])
    return data

def findInList(url):
  soup = BeautifulSoup(requests.get(url).text, "html.parser")
  list = soup.find('ul', { 'class' : 'three_columns' })
  urls = set()
  if (list == None):
    return urls
  for a in list.select('a[href^="https://codigo-postal.co/argentina/"]'):
    urls.add(a['href'])
  return urls

def findCities(soup):
  list = soup.find('ul', { 'class' : 'cities' })
  urls = set()
  if (list == None):
    return urls
  for a in list.select('a[href^="https://codigo-postal.co/argentina/"]'):
    urls.add(a['href'])
  return urls


provincias = {'https://codigo-postal.co/argentina/buenos-aires/', 'https://codigo-postal.co/argentina/capital-federal/'}

for prov in provincias:
  print('PROVINCIA: ' + prov)
  citiesInProv = set()
  soup = BeautifulSoup(requests.get(prov).text, "html.parser")
  citiesFound = findCities(soup)
  citiesInProv = citiesInProv.union(citiesFound)

  print('cities')
  print(len(citiesInProv))

  datos  = []
  calles = set()
  while len(citiesInProv)>0:
    soup = BeautifulSoup(requests.get(citiesInProv.pop()).text, "html.parser")
    unicoCPA = unicoCPAEnLocalidad(soup)
    if (unicoCPA == None):
      calle = masDeUnCPAEnLocalidad(soup)
      calles = calles.union(calle)
    else:
      datos.append(unicoCPA)

  print('calles')
  print(len(calles))
  print('cpasunicos')
  print(len(datos))

  list = set()
  while len(calles)>0:
    proxUrl = calles.pop()
    list = list.union(findInList(proxUrl))

  print('calles found ' + str(len(list)))

  dataCPAs = []
  while len(list)>0:
    proxUrl = list.pop()
    dataCPAs = dataCPAs + findInTable(proxUrl)
  #list=findInList(proxUrl)
  #if (len(table)==0 and len(list)==0):
    #findInText(proxUrl)
  #links = links.union(table).union(list)

  print('CPAS')
  print(len(dataCPAs))

  result = re.search('https://codigo-postal.co/argentina/(.*)/', prov)
  with open(r'O_'+result.group(1)+'.txt', 'w') as fp:
    fp.write("*************")
    fp.write("\n".join(str(cp) for cp in dataCPAs))
