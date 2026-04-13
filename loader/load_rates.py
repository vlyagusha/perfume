# -*- coding: utf-8 -*-

import os
import psycopg2
import locale
import xml.etree.ElementTree as ElementTree
import requests

from dotenv import load_dotenv

load_dotenv()
locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')

xml_string = requests.get('http://www.cbr.ru/scripts/XML_daily.asp').text
root = ElementTree.fromstring(xml_string)
rate = 0
for valute in root.findall('Valute'):
    if valute.find('CharCode').text == 'USD':
        rate = float(valute.find('Value').text.replace(',', '.'))
        break
if rate == 0:
    print('USD rate not found.')
    exit(1)

connection = psycopg2.connect(
    host=os.environ.get('DATABASE_HOST'),
    port=os.environ.get('DATABASE_PORT'),
    database=os.environ.get('DATABASE_NAME'),
    user=os.environ.get('DATABASE_USER'),
    password=os.environ.get('DATABASE_PASSWORD'),
)

if not connection:
    print('Connection closed.')
    exit(1)

cursor = connection.cursor()
data = ("USD", rate, rate)
cursor.execute('insert into rates(code, rate) values (%s, %s) on conflict(code) do update set rate = %s', data)
connection.commit()
print(f'USD rate updated. New rate: {rate}')

if connection:
    cursor.close()
    connection.close()
    print('Connection closed.')
