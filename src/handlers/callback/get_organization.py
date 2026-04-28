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


@router.callback_query(lambda c: c.data == "my_organization")
async def get_organization(callback: CallbackQuery, state: FSMContext | None = None) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("ПОЛУЧЕН ЗАПРОС НА ПРОФИЛЬ ОРГАНИЗАЦИИ", extra={"body": callback.from_user.id})
    user_id = callback.from_user.id
    organization = None

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

        body = {"id": user_id, "action": "get_organization"}
        await exchange.publish(
            aio_pika.Message(msgpack.packb(body)),
            routing_key="user_messages",
        )
        logger.info("ОТПРАВИЛИ ЗАПРОС НА ПРОФИЛЬ ОРГАНИЗАЦИИ В БД", extra={"body": callback.from_user.id})

        for _ in range(10):
            try:
                res = await user_queue.get()
                await res.ack()
                organization = msgpack.unpackb(res.body)
                break
            except QueueEmpty:
                logger.info("ОТВЕТ ОТ БД НЕ ПОЛУЧЕН, ОЧЕРЕДЬ ПУСТА", extra={"body": callback.from_user.id})
                await asyncio.sleep(1)

    if not organization or "error" in organization:
        await callback.answer("Профиль организации не найден", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Удалить профиль", callback_data="delete_organization"
                ),
                InlineKeyboardButton(
                    text="Редактировать профиль", callback_data="edit_organization"
                ),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="organization_back")],
        ]
    )
    if state:
        await state.update_data(profile_role="organizer")

    await callback.message.answer(
        render("profile_organization.jinja2", user=organization),
        reply_markup=keyboard,
    )
    logger.info("ОТПРАВИЛИ ПРОФИЛЬ ОРГАНИЗАЦИИ", extra={"body": callback.from_user.id})
    await callback.answer()


@router.callback_query(lambda c: c.data == "organization_back")
async def organization_back(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    logger.info("ОТПРАВИЛИ НАЗАД В МЕНЮ")
    await callback.message.answer("Меню бота:", reply_markup=build_menu_by_role("organizer"))
    await callback.answer()
