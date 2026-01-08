# -*- coding: utf-8 -*-

import os
import zipfile
import imap_tools
import pandas as pd
import psycopg2
import math

################## Configuration ######################

IMAP_SERVER = 'imap.yandex.ru'
EMAIL_USER = 'kikkershop@yandex.ru'
EMAIL_PASS = 'rzkizhonztferzmi'
CONTRACTOR_EMAIL = 'price@igells.ru'
XLS_PATH = 'data/artem/'
if not os.path.exists(XLS_PATH):
    os.makedirs(XLS_PATH)

################# Checking e-mail #####################

got_new_email = False
with imap_tools.MailBox(IMAP_SERVER).login(EMAIL_USER, EMAIL_PASS, 'INBOX') as mailbox:
    for msg in mailbox.fetch('ALL', reverse=True):
        if msg.from_ != CONTRACTOR_EMAIL:
            continue
        got_new_email = True
        today = msg.date
        for att in msg.attachments:
            if att.filename.endswith('.xls'):
                with open(XLS_PATH + 'artem.xls', 'wb') as f:
                    f.write(att.payload)
            if att.filename.endswith('.zip'):
                with open(XLS_PATH + 'otliv.zip', 'wb') as f:
                    f.write(att.payload)
                with zipfile.ZipFile(XLS_PATH + 'otliv.zip', 'r') as zip_ref:
                    zip_ref.extract(zip_ref.filelist[0].filename, XLS_PATH)
                    os.rename(XLS_PATH + zip_ref.filelist[0].filename, XLS_PATH + 'otliv.xls')
                os.remove(XLS_PATH + 'otliv.zip')
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

contractor = 'Артем'
cursor.execute('delete from prices where contractor = %s', (contractor, ))
connection.commit()

for filename in [XLS_PATH + 'artem.xls', XLS_PATH + 'otliv.xls']:
    price = pd.read_excel(filename)
    products_to_insert = []
    prices_to_insert = []
    batch_size = 5000
    for index, row in price.iterrows():
        if index < 8:
            continue

        code = str(row[0])
        title = str(row[6])
        if math.isnan(row[15]):
            price_rub = None
        else:
            price_rub = float(row[15])

        prices_to_insert.append([contractor, code, title, price_rub, today])
        if len(prices_to_insert) >= batch_size:
            args = ','.join(cursor.mogrify('(%s,%s,%s,%s,%s)', i).decode('utf-8') for i in prices_to_insert)
            cursor.execute('insert into prices(contractor, code, title, price_rub, updated_at) values ' + args)
            connection.commit()
            print('Inserted ', len(prices_to_insert), ' prices')
            prices_to_insert = []

    args = ','.join(cursor.mogrify('(%s,%s,%s,%s,%s)', i).decode('utf-8') for i in prices_to_insert)
    cursor.execute('insert into prices(contractor, code, title, price_rub, updated_at) values ' + args)
    connection.commit()
    print('Inserted ', len(prices_to_insert), ' prices')

if connection:
    cursor.close()
    connection.close()
    print('Connection closed.')

################## Finish ######################
