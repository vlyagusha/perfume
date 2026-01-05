# -*- coding: utf-8 -*-

import imap_tools
import pandas as pd
import psycopg2

################## Configuration ######################

IMAP_SERVER = 'imap.yandex.ru'
EMAIL_USER = 'kikkershop@yandex.ru'
EMAIL_PASS = 'rzkizhonztferzmi'
CONTRACTOR_EMAIL = 'cska-love@mail.ru'
XLS_PATH = 'data/yr/yr.xls'

################# Checking e-mail #####################

got_new_email = False
with imap_tools.MailBox(IMAP_SERVER).login(EMAIL_USER, EMAIL_PASS, 'INBOX') as mailbox:
    for msg in mailbox.fetch('ALL', reverse=True):
        if msg.from_ != CONTRACTOR_EMAIL:
            continue
        got_new_email = True
        today = msg.date
        for att in msg.attachments:
            print(att.filename, att.content_type)
            with open(XLS_PATH, 'wb') as f:
                f.write(att.payload)
        break

if not got_new_email:
    print('No new email found')
    exit(0)

################# Uploading to DB #####################

connection = psycopg2.connect(
    host='79.174.88.163',
    port='17041',
    database='parfum',
    user='vlyagusha',
    password='Sh19+ZnSh19+Zn',
)

if not connection:
    print('Connection closed.')
    exit(1)

cursor = connection.cursor()

contractor = 'Яр'
batch_size = 5000

cursor.execute('update products set is_available = false where contractor = %s', (contractor, ))
connection.commit()

price = pd.read_excel(XLS_PATH)
products_to_insert = []
prices_to_insert = []
for index, row in price.iterrows():
    if not str(row[0]).startswith('YR'):
        continue
    code = str(row[0])
    title = str(row[1])
    price_usd = float(row[2])

    # print(code, title, price_usd)

    products_to_insert.append([contractor, code, title, True, ])
    prices_to_insert.append([code, title, price_usd, today])

    if len(products_to_insert) >= batch_size:
        args = ','.join(cursor.mogrify('(%s,%s,%s,%s)', i).decode('utf-8') for i in products_to_insert)
        cursor.execute(
            'insert into products(contractor, code, title, is_available) values ' + args + ' on conflict(code, title) do update set is_available = true')
        connection.commit()
        print('Inserted ', len(products_to_insert), ' products')
        products_to_insert = []

    if len(prices_to_insert) >= batch_size:
        args = ','.join(cursor.mogrify('(%s,%s,%s,%s)', i).decode('utf-8') for i in prices_to_insert)
        cursor.execute('insert into prices(code, title, price_usd, updated_at) values ' + args + ' on conflict do nothing')
        connection.commit()
        print('Inserted ', len(prices_to_insert), ' prices')
        prices_to_insert = []

args = ','.join(cursor.mogrify('(%s,%s,%s,%s)', i).decode('utf-8') for i in products_to_insert)
cursor.execute(
    'insert into products(contractor, code, title, is_available) values ' + args + ' on conflict(code, title) do update set is_available = true')
connection.commit()
print('Inserted ', len(products_to_insert), ' products')

args = ','.join(cursor.mogrify('(%s,%s,%s,%s)', i).decode('utf-8') for i in prices_to_insert)
cursor.execute('insert into prices(code, title, price_usd, updated_at) values ' + args + ' on conflict do nothing')
connection.commit()
print('Inserted ', len(prices_to_insert), ' prices')

if connection:
    cursor.close()
    connection.close()
    print('Connection closed.')

################## Finish ######################
