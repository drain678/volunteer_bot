import asyncio

import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from config.settings import settings
from src.handlers.callback.router import router
from src.handlers.command.menu import build_menu_by_role
from src.storage.rabbit import channel_pool


async def _request_to_consumer(user_id: int) -> dict | None:
    body = {"id": user_id, "action": "get_tops"}
    async with channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        queue = await channel.declare_queue("user_messages", durable=True)
        user_queue = await channel.declare_queue(
            settings.USER_QUEUE.format(user_id=user_id), durable=True
        )
        await queue.bind(exchange, "user_messages")
        await user_queue.bind(exchange, settings.USER_QUEUE.format(user_id=user_id))
        await exchange.publish(aio_pika.Message(msgpack.packb(body)), routing_key="user_messages")
        for _ in range(10):
            try:
                res = await user_queue.get(timeout=3)
                await res.ack()
                return msgpack.unpackb(res.body)
            except QueueEmpty:
                await asyncio.sleep(1)
    return None


def _back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="tops_back")]]
    )


@router.callback_query(lambda c: c.data == "tops")
async def get_tops(callback: CallbackQuery) -> None:
    response = await _request_to_consumer(callback.from_user.id)
    if not response or "error" in response:
        await callback.answer("Не удалось получить топ волонтеров", show_alert=True)
        return
    tops = response.get("tops", [])
    if not tops:
        await callback.message.answer("Пока нет волонтеров для рейтинга.", reply_markup=_back_keyboard())
        await callback.answer()
        return

    medals = {0: "🥇", 1: "🥈", 2: "🥉"}
    lines = ["🏆 Топ волонтеров:"]
    for idx, item in enumerate(tops, start=1):
        medal = f"{medals[idx - 1]} " if idx - 1 in medals else ""
        name = item.get("name") or "Без имени"
        rating = item.get("rating") or 0
        lines.append(f"{idx}. {medal}{name} — рейтинг {rating:.2f}")

    await callback.message.answer("\n".join(lines), reply_markup=_back_keyboard())
    await callback.answer()


@router.callback_query(lambda c: c.data == "tops_back")
async def tops_back(callback: CallbackQuery) -> None:
    await callback.message.answer("Меню бота:", reply_markup=build_menu_by_role("volunteer"))
    await callback.answer()
