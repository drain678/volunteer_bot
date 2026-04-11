import asyncio
import logging.config

import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from config.settings import settings
from src.handlers.callback.router import router
# from src.logger import LOGGING_CONFIG, get_logger
from src.storage.rabbit import channel_pool
from src.templates.env import render

# logger = get_logger()

@router.callback_query(lambda c: c.data == "profile")
async def get_profile(callback: CallbackQuery) -> None:
    # logging.config.dictConfig(LOGGING_CONFIG)

    user_id = callback.from_user.id

    async with channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )

        queue = await channel.declare_queue("user_messages", durable=True)

        user_queue = await channel.declare_queue(
            settings.USER_QUEUE.format(user_id=user_id),
            durable=True
        )

        await queue.bind(exchange, 'user_messages')
        await user_queue.bind(exchange, settings.USER_QUEUE.format(user_id=user_id))

        body = {"id": user_id, "action": "get_profile"}

        await exchange.publish(
            aio_pika.Message(msgpack.packb(body)),
            "user_messages"
        )

        # ждём ответ
        for _ in range(3):
            try:
                res = await user_queue.get()
                await res.ack()

                profile = msgpack.unpackb(res.body)

                if "error" in profile:
                    await callback.answer("Профиль не найден", show_alert=True)
                    return

                buttons = [
                    [
                        InlineKeyboardButton(
                            text="Изменить профиль",
                            callback_data="edit_profile",
                        ),
                        InlineKeyboardButton(
                            text="Удалить профиль",
                            callback_data="delete_profile",
                        ),
                    ]
                ]

                keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

                await callback.message.answer(
                    render("profile.jinja2", user=profile),
                    reply_markup=keyboard,
                )

                await callback.answer()
                return

            except QueueEmpty:
                await asyncio.sleep(1)

    await callback.answer("Ошибка получения профиля", show_alert=True)
