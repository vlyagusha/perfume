# -*- coding: utf-8 -*-
from os.path import isfile

import imap_tools
import pandas as pd
import psycopg2

################## Configuration ######################

IMAP_SERVER = 'imap.yandex.ru'
EMAIL_USER = 'kikkershop@yandex.ru'
EMAIL_PASS = 'rzkizhonztferzmi'
CONTRACTOR_EMAIL = 'piramida9999@mail.ru'
XLS_PATH = 'data/piramida9999/piramida9999.xls'

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

contractor = 'Маргарита'
batch_size = 5000

cursor.execute('delete from prices where contractor = %s', (contractor, ))
connection.commit()

price = pd.read_excel(XLS_PATH)
prices_to_insert = []
for index, row in price.iterrows():
    if not str(row[0]).isdigit():
        continue
    code = str(row[0])
    title = str(row[1])
    price_usd = float(row[2])

    prices_to_insert.append([contractor, code, title, price_usd, today])
    if len(prices_to_insert) >= batch_size:
        args = ','.join(cursor.mogrify('(%s,%s,%s,%s,%s)', i).decode('utf-8') for i in prices_to_insert)
        cursor.execute('insert into prices(contractor, code, title, price_usd, updated_at) values ' + args)
        connection.commit()
        print('Inserted ', len(prices_to_insert), ' prices')
        prices_to_insert = []

args = ','.join(cursor.mogrify('(%s,%s,%s,%s,%s)', i).decode('utf-8') for i in prices_to_insert)
cursor.execute('insert into prices(contractor, code, title, price_usd, updated_at) values ' + args)
connection.commit()
print('Inserted ', len(prices_to_insert), ' prices')

if connection:
    cursor.close()
    connection.close()
    print('Connection closed.')

################## Finish ######################
