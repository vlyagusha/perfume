import asyncio
import logging
import os

from dotenv import load_dotenv
from maxapi import Bot, Dispatcher, F
from maxapi.types import MessageCreated, BotStarted, Command

logging.basicConfig(filename='bot.log', level=logging.WARNING)

load_dotenv()

bot = Bot(os.environ.get('MAX_BOT_TOKEN'))
dp = Dispatcher()

@dp.message_created(F.message.body.text)
async def echo(event: MessageCreated):
    await event.message.answer(f"Повторяю за вами: {event.message.body.text}")

@dp.bot_started()
async def bot_started(event: BotStarted):
    await event.bot.send_message(
        chat_id=event.chat_id,
        text='Привет! Отправь мне /start'
    )

@dp.message_created(Command('start'))
async def hello(event: MessageCreated):
    await event.message.answer(f"Пример чат-бота для MAX 💙")

async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
