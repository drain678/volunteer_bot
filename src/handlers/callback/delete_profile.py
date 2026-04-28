import asyncio

import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from config.settings import settings
from src.handlers.callback.router import router
from src.storage.rabbit import channel_pool


async def request_to_consumer(payload: dict) -> dict | None:
    user_id = payload["id"]
    async with channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        queue = await channel.declare_queue("user_messages", durable=True)
        user_queue = await channel.declare_queue(
            settings.USER_QUEUE.format(user_id=user_id), durable=True
        )
        await queue.bind(exchange, "user_messages")
        await user_queue.bind(exchange, settings.USER_QUEUE.format(user_id=user_id))

        await exchange.publish(
            aio_pika.Message(msgpack.packb(payload)),
            "user_messages",
        )

        for _ in range(10):
            try:
                res = await user_queue.get()
                await res.ack()
                return msgpack.unpackb(res.body)
            except QueueEmpty:
                await asyncio.sleep(1)
    return None


@router.callback_query(lambda c: c.data == "delete_profile")
async def ask_delete_profile(callback: CallbackQuery) -> None:
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да, удалить", callback_data="delete_profile_yes"),
                InlineKeyboardButton(text="Нет", callback_data="delete_profile_no"),
            ]
        ]
    )
    await callback.message.answer("Ты точно хочешь удалить свой профиль?", reply_markup=keyboard)
    await callback.answer()


@router.callback_query(lambda c: c.data == "delete_profile_no")
async def cancel_delete_profile(callback: CallbackQuery) -> None:
    await callback.message.answer("Удаление отменено.")
    await callback.answer()


@router.callback_query(lambda c: c.data == "delete_profile_yes")
async def confirm_delete_profile(callback: CallbackQuery, state: FSMContext) -> None:
    result = await request_to_consumer(
        {"id": callback.from_user.id, "action": "delete_profile"}
    )
    if not result or "error" in result:
        await callback.message.answer("Не удалось удалить профиль. Попробуй позже.")
        await callback.answer()
        return

    await state.clear()
    await callback.message.answer("Профиль удален.")
    await callback.answer()
