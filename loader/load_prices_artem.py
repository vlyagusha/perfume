# -*- coding: utf-8 -*-

import os
import zipfile
import imap_tools
import pandas as pd
import psycopg2
import math

from dotenv import load_dotenv

load_dotenv()

################## Configuration ######################

IMAP_SERVER = os.environ.get('IMAP_SERVER')
EMAIL_USER = os.environ.get('EMAIL_USER')
EMAIL_PASS = os.environ.get('EMAIL_PASS')
CONTRACTOR_EMAIL = 'price@igells.ru'
XLS_PATH = '../data/artem/'
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
        print(today)
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
    host=os.environ.get('HOST'),
    port=os.environ.get('PORT'),
    database=os.environ.get('DATABASE'),
    user=os.environ.get('USER'),
    password=os.environ.get('PASSWORD'),
)

if not connection:
    print('Connection closed.')
    exit(1)

cursor = connection.cursor()

contractor = 'Артем'
cursor.execute('delete from prices where contractor = %s', (contractor,))
connection.commit()

for filename in [XLS_PATH + 'artem.xls', XLS_PATH + 'otliv.xls']:
    price = pd.read_excel(filename)
    products_to_insert = []
    prices_to_insert = []
    batch_size = 5000
    for index, row in price.iterrows():
        if index < 8:
            continue

        code = str(row.iloc[0])
        title = str(row.iloc[6])
        if math.isnan(row.iloc[15]):
            price_rub = None
        else:
            price_rub = float(row.iloc[15])

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
