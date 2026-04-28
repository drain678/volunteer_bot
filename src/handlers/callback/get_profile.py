import asyncio
import logging.config

import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from consumer.logger import LOGGING_CONFIG, logger
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from config.settings import settings
from src.handlers.command.menu import build_menu_by_role
from src.handlers.callback.router import router
from src.storage.rabbit import channel_pool
from src.templates.env import render


@router.callback_query(lambda c: c.data == "profile")
async def get_profile(callback: CallbackQuery, state: FSMContext) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("ПОЛУЧЕН ЗАПРОС НА ПРОФИЛЬ ИЗ SRC", extra={"body": callback.from_user.id})
    user_id = callback.from_user.id
    profile = None
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

        body = {"id": user_id, "action": "get_profile"}
        await exchange.publish(aio_pika.Message(msgpack.packb(body)), routing_key="user_messages")
        logger.info("ОТПРАВИЛИ ЗАПРОС НА ПРОФИЛЬ ИЗ SRC ЧЕРЕЗ ОЧЕРЕДЬ")

        for _ in range(10):
            try:
                res = await user_queue.get()
                await res.ack()
                profile = msgpack.unpackb(res.body)
                break
            except QueueEmpty:
                await asyncio.sleep(1)

    if not profile or "error" in profile:
        await callback.answer("Профиль не найден", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Удалить профиль", callback_data="delete_profile"),
                InlineKeyboardButton(
                    text="Редактировать профиль", callback_data="edit_profile"
                ),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="profile_back")],
        ]
    )

    await state.clear()
    await state.update_data(profile_role=profile.get("role", "volunteer"))
    template_name = (
        "profile_organization.jinja2"
        if profile.get("role") == "organizer"
        else "profile.jinja2"
    )
    await callback.message.answer(render(template_name, user=profile), reply_markup=keyboard)
    await callback.answer()


@router.callback_query(lambda c: c.data == "profile_back")
async def profile_back(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    role = data.get("profile_role", "volunteer")
    await state.clear()
    await callback.message.answer("Меню бота:", reply_markup=build_menu_by_role(role))
    await callback.answer()



@router.callback_query(lambda c: c.data == "back_to_profile")
async def back_to_profile(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    role = data.get("profile_role", "volunteer")
    await state.clear()
    await get_profile(callback, state)
    await callback.answer()