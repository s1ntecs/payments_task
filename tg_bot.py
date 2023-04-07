import asyncio
import os
import json
import aiogram
from mongo_worker import aggregate_payments
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
messages = []


async def handle_message(message: aiogram.types.Message):
    try:
        data = json.loads(message.text)
        dt_from = data.get("dt_from")
        dt_upto = data.get("dt_upto")
        group_type = data.get("group_type")
        result = await aggregate_payments(dt_from, dt_upto, group_type)
    except json.JSONDecodeError:
        result = """
            Invalid message format. Message should be in the following format:
            {\"dt_from\": \"2022-09-01T00:00:00\", \"dt_upto\":
            \"2022-12-31T23:59:00\", \"group_type\": \"month\"}
        """
    await bot.send_message(message.from_user.id, result)


bot = aiogram.Bot(token=TELEGRAM_TOKEN)

dp = aiogram.Dispatcher(bot)

dp.register_message_handler(handle_message)


async def main():
    await dp.start_polling()

if __name__ == '__main__':
    asyncio.run(main())
