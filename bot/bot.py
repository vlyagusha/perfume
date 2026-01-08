# -*- coding: utf-8 -*-

import os

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from dotenv import load_dotenv

load_dotenv()

async def hello(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    print(update.message.text)
    await update.message.reply_text(f'Hello {update.effective_user.first_name}')


app = ApplicationBuilder().token(os.environ.get('BOT_TOKEN')).build()

app.add_handler(CommandHandler("hello", hello))

app.run_polling()
