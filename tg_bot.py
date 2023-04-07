import asyncio
import os
import json
import aiogram
from mongo_worker import aggregate_payments
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")


async def handle_message(message: aiogram.types.Message):
    try:
        data = json.loads(message.text)
        dt_from = data.get("dt_from")
        dt_upto = data.get("dt_upto")
        group_type = data.get("group_type")
        result = await aggregate_payments(dt_from, dt_upto, group_type)
        json_res = json.dumps(result)
    except json.JSONDecodeError:
        result = """
            Invalid message format. Message should be in the following format:
            {"dt_from": "2022-09-01T00:00:00", "dt_upto":
            "2022-12-31T23:59:00", "group_type": "month"}
        """
    await message.answer(json_res)


async def handle_start(message: aiogram.types.Message):
    await message.answer(f"Hello, {message.from_user.first_name}!")


async def main():
    bot = aiogram.Bot(token=TELEGRAM_TOKEN)
    dp = aiogram.Dispatcher(bot)
    dp.register_message_handler(handle_start, commands=["start"])
    dp.register_message_handler(handle_message)
    await dp.start_polling()

if __name__ == '__main__':
    asyncio.run(main())
