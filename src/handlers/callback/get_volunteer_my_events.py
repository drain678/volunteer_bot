import asyncio

import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config.settings import settings
from src.handlers.callback.router import router
from src.storage.rabbit import channel_pool
from src.templates.env import render

PAGE_SIZE = 4


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


def _events_keyboard(kind: str, page: int, total_pages: int) -> InlineKeyboardMarkup:
    prev_page = (page - 1) % total_pages
    next_page = (page + 1) % total_pages
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️", callback_data=f"volunteer_events_page_{kind}_{prev_page}"),
                InlineKeyboardButton(text="Назад", callback_data="volunteer_my_events"),
                InlineKeyboardButton(text="➡️", callback_data=f"volunteer_events_page_{kind}_{next_page}"),
            ]
        ]
    )


def _event_card_keyboard(kind: str, event_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Назад", callback_data=f"volunteer_events_page_{kind}_{page}"),
            ]
        ]
    )


async def _safe_edit(callback: CallbackQuery, text: str, keyboard: InlineKeyboardMarkup | None = None) -> None:
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc):
            raise


def _build_event_link(bot_username: str | None, kind: str, event_id: int, page: int) -> str:
    if bot_username:
        return f"https://t.me/{bot_username}?start=vmy_event_{kind}_{event_id}_{page}"
    return "https://t.me/"


async def _get_volunteer_events(user_id: int, kind: str) -> list[dict] | None:
    response = await _request_to_consumer(
        user_id,
        "get_volunteer_my_events",
        {"kind": kind},
    )
    if not response or "error" in response:
        return None
    return response.get("events", [])


async def _show_list(callback: CallbackQuery, kind: str, page: int) -> None:
    if kind not in {"upcoming", "past"}:
        await callback.answer("Некорректный раздел", show_alert=True)
        return
    events = await _get_volunteer_events(callback.from_user.id, kind)
    if events is None:
        await callback.answer("Не удалось получить мероприятия", show_alert=True)
        return
    if not events:
        label = "предстоящих" if kind == "upcoming" else "прошедших"
        await _safe_edit(callback, f"У вас пока нет {label} мероприятий.")
        await callback.answer()
        return

    total_pages = (len(events) + PAGE_SIZE - 1) // PAGE_SIZE
    page = page % total_pages
    start = page * PAGE_SIZE
    chunk = events[start:start + PAGE_SIZE]
    me = await callback.bot.get_me()
    bot_username = me.username
    prepared_events = []
    for event in chunk:
        prepared = dict(event)
        prepared["link"] = _build_event_link(bot_username, kind, int(event["id"]), page)
        prepared_events.append(prepared)

    title = "Предстоящие мероприятия" if kind == "upcoming" else "Прошедшие мероприятия"
    await _safe_edit(
        callback,
        render(
            "volunteer_my_events_list.jinja2",
            title=title,
            events=prepared_events,
            page=page + 1,
            total_pages=total_pages,
            total_events=len(events),
        ),
        _events_keyboard(kind=kind, page=page, total_pages=total_pages),
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
    await _show_list(callback, kind="upcoming", page=0)


@router.callback_query(lambda c: c.data == "volunteer_events_past")
async def volunteer_events_past(callback: CallbackQuery) -> None:
    await _show_list(callback, kind="past", page=0)


@router.callback_query(lambda c: c.data.startswith("volunteer_events_page_"))
async def volunteer_events_page(callback: CallbackQuery) -> None:
    try:
        _, _, _, kind, page_raw = callback.data.split("_", 4)
        page = int(page_raw)
    except (ValueError, IndexError):
        await callback.answer("Некорректная страница", show_alert=True)
        return
    await _show_list(callback, kind=kind, page=page)


async def send_volunteer_event_card(
    message: Message,
    user_id: int,
    kind: str,
    event_id: int,
    page: int = 0,
) -> bool:
    events = await _get_volunteer_events(user_id, kind)
    if events is None:
        await message.answer("Не удалось получить мероприятия.")
        return False
    event = next((item for item in events if int(item.get("id", -1)) == event_id), None)
    if not event:
        await message.answer("Мероприятие не найдено.")
        return False
    await message.answer(
        render("event.jinja2", event=event),
        reply_markup=_event_card_keyboard(kind=kind, event_id=event_id, page=page),
    )
    return True
