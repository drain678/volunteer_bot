import asyncio

import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from config.settings import settings
from src.handlers.callback.router import router
from src.storage.rabbit import channel_pool
from src.templates.env import render


async def _request_to_consumer(user_id: int, action: str, payload: dict | None = None) -> dict | None:
    body = {"id": user_id, "action": action}
    if payload:
        body.update(payload)
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


def _choose_kind_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Предстоящие", callback_data="volunteer_events_upcoming"),
                InlineKeyboardButton(text="Прошедшие", callback_data="volunteer_events_past"),
            ]
        ]
    )


def _events_keyboard(kind: str, index: int, total: int) -> InlineKeyboardMarkup:
    prev_index = (index - 1) % total
    next_index = (index + 1) % total
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️", callback_data=f"volunteer_events_page_{kind}_{prev_index}"),
                InlineKeyboardButton(text="Назад", callback_data="volunteer_my_events"),
                InlineKeyboardButton(text="➡️", callback_data=f"volunteer_events_page_{kind}_{next_index}"),
            ]
        ]
    )


async def _safe_edit(callback: CallbackQuery, text: str, keyboard: InlineKeyboardMarkup | None = None) -> None:
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc):
            raise


async def _show_list(callback: CallbackQuery, kind: str, index: int) -> None:
    response = await _request_to_consumer(
        callback.from_user.id,
        "get_volunteer_my_events",
        {"kind": kind},
    )
    if not response or "error" in response:
        await callback.answer("Не удалось получить мероприятия", show_alert=True)
        return
    events = response.get("events", [])
    if not events:
        label = "предстоящих" if kind == "upcoming" else "прошедших"
        await _safe_edit(callback, f"У вас пока нет {label} мероприятий.")
        await callback.answer()
        return
    index = index % len(events)
    event = events[index]
    await _safe_edit(
        callback,
        render("event.jinja2", event=event),
        _events_keyboard(kind=kind, index=index, total=len(events)),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "volunteer_my_events")
async def volunteer_my_events(callback: CallbackQuery) -> None:
    await _safe_edit(
        callback,
        "Выберите раздел:",
        _choose_kind_keyboard(),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "volunteer_events_upcoming")
async def volunteer_events_upcoming(callback: CallbackQuery) -> None:
    await _show_list(callback, kind="upcoming", index=0)


@router.callback_query(lambda c: c.data == "volunteer_events_past")
async def volunteer_events_past(callback: CallbackQuery) -> None:
    await _show_list(callback, kind="past", index=0)


@router.callback_query(lambda c: c.data.startswith("volunteer_events_page_"))
async def volunteer_events_page(callback: CallbackQuery) -> None:
    try:
        _, _, _, kind, idx = callback.data.split("_", 4)
        index = int(idx)
    except (ValueError, IndexError):
        await callback.answer("Некорректный индекс", show_alert=True)
        return
    await _show_list(callback, kind=kind, index=index)
