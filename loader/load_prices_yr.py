# -*- coding: utf-8 -*-

import os
import imap_tools
import pandas as pd
import psycopg2

from dotenv import load_dotenv

load_dotenv()

################## Configuration ######################

IMAP_SERVER = os.environ.get('IMAP_SERVER')
EMAIL_USER = os.environ.get('EMAIL_USER')
EMAIL_PASS = os.environ.get('EMAIL_PASS')
CONTRACTOR_EMAIL = 'cska-love@mail.ru'
XLS_PATH = 'data/yr/'
if not os.path.exists(XLS_PATH):
    os.makedirs(XLS_PATH)
XLS_PATH += 'yr.xls'

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
            print(att.filename)
            with open(XLS_PATH, 'wb') as f:
                f.write(att.payload)
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

contractor = 'Яр'
batch_size = 5000

cursor.execute('delete from prices where contractor = %s', (contractor, ))
connection.commit()

price = pd.read_excel(XLS_PATH)
prices_to_insert = []
for index, row in price.iterrows():
    if not str(row.iloc[0]).startswith('YR'):
        continue
    code = str(row.iloc[0])
    title = str(row.iloc[1])
    price_usd = float(row.iloc[2])

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
