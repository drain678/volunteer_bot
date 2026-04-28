import asyncio
import logging
import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from consumer.logger import LOGGING_CONFIG, logger
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from config.settings import settings
from src.handlers.callback.router import router
from src.storage.rabbit import channel_pool
from src.templates.env import render


async def _request_to_consumer(user_id: int) -> dict | None:
    logging.config.dictConfig(LOGGING_CONFIG)
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

        body = {"id": user_id, "action": "get_organizations"}
        await exchange.publish(
            aio_pika.Message(msgpack.packb(body)),
            routing_key="user_messages",
        )
        logger.info("ОТПРАВИЛИ ЗАПРОС НА СПИСОК ОРГАНИЗАЦИЙ В БД", extra={"body": user_id})

        for _ in range(10):
            try:
                res = await user_queue.get()
                await res.ack()
                return msgpack.unpackb(res.body)
            except QueueEmpty:
                await asyncio.sleep(1)
    return None


def _organizations_keyboard(index: int, total: int) -> InlineKeyboardMarkup:
    prev_index = (index - 1) % total
    next_index = (index + 1) % total
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️", callback_data=f"organizations_{prev_index}"),
                InlineKeyboardButton(text="Мероприятия", callback_data=f"organization_events_{index}"),
                InlineKeyboardButton(text="➡️", callback_data=f"organizations_{next_index}"),
            ]
        ]
    )


async def _show_organization_by_index(callback: CallbackQuery, index: int) -> None:
    response = await _request_to_consumer(callback.from_user.id)
    if not response or "error" in response:
        await callback.answer("Не удалось получить список организаций", show_alert=True)
        return

    organizations = response.get("organizations", [])
    if not organizations:
        await callback.answer("Пока нет организаций", show_alert=True)
        return

    index = index % len(organizations)
    organization = organizations[index]
    keyboard = _organizations_keyboard(index=index, total=len(organizations))

    await callback.message.answer(
        render("organization.jinja2", user=organization),
        reply_markup=keyboard,
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "organizations")
async def get_organizations(callback: CallbackQuery) -> None:
    await _show_organization_by_index(callback, index=0)


@router.callback_query(lambda c: c.data.startswith("organizations_"))
async def paginate_organizations(callback: CallbackQuery) -> None:
    try:
        index = int(callback.data.split("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный индекс", show_alert=True)
        return
    await _show_organization_by_index(callback, index=index)
