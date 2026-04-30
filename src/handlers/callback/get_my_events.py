import asyncio
import logging
import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from consumer.logger import LOGGING_CONFIG, logger
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config.settings import settings
from src.handlers.callback.router import router
from src.handlers.state.delete_event import DeleteEventState
from src.storage.rabbit import channel_pool
from src.templates.env import render


async def _request_to_consumer(user_id: int, action: str, payload: dict | None = None) -> dict | None:
    logging.config.dictConfig(LOGGING_CONFIG)
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

        await exchange.publish(
            aio_pika.Message(msgpack.packb(body)),
            routing_key="user_messages",
        )
        logger.info("ОТПРАВИЛИ ЗАПРОС НА ПОЛУЧЕНИЕ МОИХ МЕРОПРИЯТИЙ В БД", extra={"body": user_id})

        for _ in range(10):
            try:
                res = await user_queue.get(timeout=3)
                await res.ack()
                return msgpack.unpackb(res.body)
            except QueueEmpty:
                await asyncio.sleep(1)
    return None


async def _safe_edit_text(callback: CallbackQuery, text: str, keyboard: InlineKeyboardMarkup) -> None:
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc):
            raise


def _my_events_keyboard(index: int, total: int) -> InlineKeyboardMarkup:
    prev_index = (index - 1) % total
    next_index = (index + 1) % total
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️", callback_data=f"my_events_{prev_index}"),
                InlineKeyboardButton(text="Участники", callback_data=f"my_event_participants_{index}"),
                InlineKeyboardButton(text="➡️", callback_data=f"my_events_{next_index}"),
            ],
            [InlineKeyboardButton(text="Удалить мероприятие", callback_data=f"my_event_delete_{index}")],
        ]
    )


async def _show_my_event_by_index(callback: CallbackQuery, index: int) -> None:
    response = await _request_to_consumer(callback.from_user.id, "get_my_events")
    if not response or "error" in response:
        await callback.answer("Не удалось получить мероприятия", show_alert=True)
        return

    events = response.get("events", [])
    if not events:
        await callback.answer("У вас пока нет мероприятий", show_alert=True)
        return

    index = index % len(events)
    event = events[index]
    keyboard = _my_events_keyboard(index=index, total=len(events))

    await _safe_edit_text(
        callback,
        render("my_event.jinja2", event=event),
        keyboard,
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "my_events")
async def get_my_events(callback: CallbackQuery) -> None:
    await _show_my_event_by_index(callback, index=0)


@router.callback_query(lambda c: c.data.startswith("my_events_"))
async def paginate_my_events(callback: CallbackQuery) -> None:
    try:
        index = int(callback.data.split("_", 2)[2])
    except (ValueError, IndexError):
        await callback.answer("Некорректный индекс", show_alert=True)
        return
    await _show_my_event_by_index(callback, index=index)


@router.callback_query(lambda c: c.data.startswith("my_event_participants_"))
async def get_event_participants(callback: CallbackQuery) -> None:
    try:
        index = int(callback.data.split("_", 3)[3])
    except (ValueError, IndexError):
        await callback.answer("Некорректный индекс", show_alert=True)
        return

    events_response = await _request_to_consumer(callback.from_user.id, "get_my_events")
    if not events_response or "error" in events_response:
        await callback.answer("Не удалось получить мероприятия", show_alert=True)
        return

    events = events_response.get("events", [])
    if not events:
        await callback.answer("У вас пока нет мероприятий", show_alert=True)
        return

    index = index % len(events)
    event = events[index]
    participants_response = await _request_to_consumer(
        callback.from_user.id,
        "get_event_participants",
        {"event_id": event.get("id")},
    )
    if not participants_response or "error" in participants_response:
        await callback.answer("Не удалось получить участников", show_alert=True)
        return

    participants = participants_response.get("participants", [])
    title = participants_response.get("event_title") or event.get("title")
    if not participants:
        await callback.message.answer(f"Для мероприятия «{title}» пока нет участников.")
        await callback.answer()
        return

    lines = [f"👥 Участники мероприятия «{title}»:"]
    for participant in participants:
        name = participant.get("name") or "Без имени"
        phone = participant.get("phone") or "без телефона"
        status = participant.get("status") or "pending"
        lines.append(f"• {name}, {phone} — {status}")

    await callback.message.answer("\n".join(lines))
    await callback.answer()


def _delete_confirm_keyboard(index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data=f"my_event_delete_yes_{index}"),
                InlineKeyboardButton(text="Нет", callback_data=f"my_event_delete_no_{index}"),
            ]
        ]
    )


@router.callback_query(
    lambda c: c.data.startswith("my_event_delete_")
    and not c.data.startswith("my_event_delete_yes_")
    and not c.data.startswith("my_event_delete_no_")
)
async def start_delete_event(callback: CallbackQuery) -> None:
    parts = callback.data.split("_")
    if len(parts) != 4:
        await callback.answer("Некорректный индекс", show_alert=True)
        return
    try:
        index = int(parts[3])
    except ValueError:
        await callback.answer("Некорректный индекс", show_alert=True)
        return
    await callback.message.answer(
        "Точно хотите удалить мероприятие?",
        reply_markup=_delete_confirm_keyboard(index),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("my_event_delete_no_"))
async def cancel_delete_event(callback: CallbackQuery) -> None:
    await callback.message.answer("Удаление отменено.")
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("my_event_delete_yes_"))
async def confirm_delete_event(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        index = int(callback.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный индекс", show_alert=True)
        return
    events_response = await _request_to_consumer(callback.from_user.id, "get_my_events")
    if not events_response or "error" in events_response:
        await callback.answer("Не удалось получить мероприятия", show_alert=True)
        return
    events = events_response.get("events", [])
    if not events:
        await callback.answer("У вас пока нет мероприятий", show_alert=True)
        return
    event = events[index % len(events)]
    await state.set_state(DeleteEventState.waiting_reason)
    await state.update_data(delete_event_id=event.get("id"), delete_event_title=event.get("title"))
    await callback.message.answer(
        "Если мероприятие еще не прошло, укажи причину отмены. "
    )
    await callback.answer()


@router.message(DeleteEventState.waiting_reason)
async def delete_event_with_reason(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    event_id = data.get("delete_event_id")
    if not event_id:
        await message.answer("Не удалось определить мероприятие.")
        await state.clear()
        return
    reason = (message.text or "").strip()
    if reason == "-":
        reason = ""
    response = await _request_to_consumer(
        message.from_user.id,
        "delete_event",
        {"event_id": event_id, "reason": reason},
    )
    if not response or "error" in response:
        if response and response.get("error") == "reason_required":
            await message.answer("Для будущего мероприятия нужно указать причину отмены.")
            return
        await message.answer("Не удалось удалить мероприятие.")
        await state.clear()
        return

    event_title = response.get("event_title") or data.get("delete_event_title") or "мероприятие"
    notify_reason = response.get("reason") or "без указания причины"
    volunteer_ids = response.get("volunteer_telegram_ids") or []
    for volunteer_id in volunteer_ids:
        try:
            await message.bot.send_message(
                volunteer_id,
                f"Мероприятие «{event_title}» отменилось по причине: {notify_reason}",
            )
        except TelegramBadRequest:
            pass
    await message.answer("Мероприятие удалено.")
    await state.clear()
