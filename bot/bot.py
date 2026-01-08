# -*- coding: utf-8 -*-

import os
import psycopg2
import locale

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from dotenv import load_dotenv

load_dotenv()
locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')

async def hello(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f'Hello {update.effective_user.first_name}')

async def search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    connection = psycopg2.connect(
        host=os.environ.get('DATABASE_HOST'),
        port=os.environ.get('DATABASE_PORT'),
        database=os.environ.get('DATABASE_NAME'),
        user=os.environ.get('DATABASE_USER'),
        password=os.environ.get('DATABASE_PASSWORD'),
    )
    if not connection:
        await update.message.reply_text('Нет подключения к БД')
        return

    query = 'select code, title, price_usd, price_rub from prices'

    need_otliv = False
    slugs = []
    for slug in update.message.text.split():
        if slug.lower() == 'отлив':
            need_otliv = True
        slugs.append(f"title ilike '%{slug}%'")

    if not need_otliv:
        slugs.append(f"title not ilike '%отлив%'")

    query += ' where '
    query += ' and '.join(slugs)
    query += ' order by coalesce(price_usd, price_rub) desc'
    query += ' limit 10'

    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()

    result = ''
    for row in cursor.fetchall():
        code = row[0]
        title = row[1]
        price_usd = float(row[2] or 0)
        price_rub = float(row[3] or 0)

        price = 0
        if not price_rub == 0:
            price = round(price_rub * 1.2, -2)

        if not price_usd == 0:
            price = round(price_usd * 80 * 1.2, -2)

        result += f'{code} {title} {locale.currency(price, grouping=True)}\n'
    if result == '':
        result = 'Ничего не нашлось'

    if connection:
        cursor.close()
        connection.close()

    await update.message.reply_text(result)

app = ApplicationBuilder().token(os.environ.get('BOT_TOKEN')).build()
app.add_handler(CommandHandler("hello", hello))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search))

app.run_polling(allowed_updates=Update.ALL_TYPES)
