import asyncio
import logging
from datetime import datetime

import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config.settings import settings
from consumer.logger import LOGGING_CONFIG, logger
from src.handlers.callback.router import router
from src.handlers.state.delete_event import DeleteEventState
from src.handlers.state.edit_event import EditEventState
from src.storage.rabbit import channel_pool
from src.templates.env import render

PAGE_SIZE = 4
EDITABLE_EVENT_FIELDS = {
    "title": "Название",
    "description": "Описание",
    "direction": "Направление",
    "city": "Город",
    "date": "Дата",
    "time": "Время",
    "duration_hours": "Длительность",
    "min_age": "Мин. возраст",
}
FIELD_VALUE_PROMPTS = {
    "title": "Введи новое название:",
    "description": "Введи новое описание:",
    "direction": "Введи новое направление:",
    "city": "Введи новый город:",
    "date": "Введи новую дату. Формат: ДД.ММ.ГГГГ",
    "time": "Введи новое время. Формат: ЧЧ:ММ",
    "duration_hours": "Введи новую длительность в часах. Пример: 2 или 2.5",
    "min_age": "Введи новый минимальный возраст:",
}


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


def _my_events_list_keyboard(page: int, total_pages: int) -> InlineKeyboardMarkup:
    prev_page = (page - 1) % total_pages
    next_page = (page + 1) % total_pages
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️", callback_data=f"my_events_page_{prev_page}"),
                InlineKeyboardButton(text="➡️", callback_data=f"my_events_page_{next_page}"),
            ]
        ]
    )


def _my_event_actions_keyboard(event_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Участники", callback_data=f"my_event_participants_{event_id}_{page}"),
                InlineKeyboardButton(text="Назад", callback_data=f"my_events_page_{page}"),
            ],
            [
                InlineKeyboardButton(text="Редактировать мероприятие", callback_data=f"my_event_edit_{event_id}_{page}"),
                InlineKeyboardButton(text="Удалить мероприятие", callback_data=f"my_event_delete_{event_id}_{page}"),
            ],
        ]
    )


def _edit_fields_keyboard(event_id: int, page: int) -> InlineKeyboardMarkup:
    field_items = list(EDITABLE_EVENT_FIELDS.items())
    rows: list[list[InlineKeyboardButton]] = []
    for idx in range(0, len(field_items), 2):
        row: list[InlineKeyboardButton] = []
        for field_key, field_label in field_items[idx:idx + 2]:
            row.append(
                InlineKeyboardButton(
                    text=field_label,
                    callback_data=f"my_event_edit_field_{field_key}_{event_id}_{page}",
                )
            )
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _edit_more_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="my_event_more_yes"),
                InlineKeyboardButton(text="Нет", callback_data="my_event_more_no"),
            ]
        ]
    )


def _parse_event_field_update(raw_field: str, raw_value: str) -> tuple[dict, str] | None:
    value = raw_value.strip()
    if not value:
        return None
    if raw_field in {"title", "description", "direction", "city"}:
        payload = {raw_field: value}
        return payload, value
    if raw_field == "date":
        try:
            datetime.strptime(value, "%d.%m.%Y")
        except ValueError:
            return None
        return {"start_date": value}, value
    if raw_field == "time":
        try:
            datetime.strptime(value, "%H:%M")
        except ValueError:
            return None
        return {"start_time": value}, value
    if raw_field == "duration_hours":
        normalized = value.replace(",", ".")
        try:
            duration_hours = float(normalized)
        except ValueError:
            return None
        if duration_hours <= 0:
            return None
        return {"duration_hours": duration_hours}, str(duration_hours)
    if raw_field == "min_age":
        if not value.isdigit():
            return None
        min_age = int(value)
        if min_age < 14 or min_age > 100:
            return None
        return {"min_age": min_age}, str(min_age)
    return None


def _notification_field_text(field_key: str, value: str) -> str:
    labels = {
        "title": ("Новое", "название"),
        "description": ("Новое", "описание"),
        "direction": ("Новое", "направление"),
        "city": ("Новый", "город"),
        "date": ("Новая", "дата"),
        "time": ("Новое", "начало"),
        "duration_hours": ("Новая", "длительность"),
        "min_age": ("Новый", "мин. возраст"),
    }
    adjective, label = labels.get(field_key, ("Новое", field_key))
    return f"{adjective} {label}: {value}"


def _build_event_link(bot_username: str | None, event_id: int, page: int) -> str:
    if bot_username:
        return f"https://t.me/{bot_username}?start=my_event_{event_id}_{page}"
    return "https://t.me/"


async def _get_my_events(user_id: int) -> list[dict] | None:
    response = await _request_to_consumer(user_id, "get_my_events")
    if not response or "error" in response:
        return None
    return response.get("events", [])


async def _show_my_events_page(callback: CallbackQuery, page: int) -> None:
    events = await _get_my_events(callback.from_user.id)
    if events is None:
        await callback.answer("Не удалось получить мероприятия", show_alert=True)
        return
    if not events:
        await callback.answer("У вас пока нет мероприятий", show_alert=True)
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
        prepared["link"] = _build_event_link(bot_username, int(event["id"]), page)
        prepared_events.append(prepared)

    text = render(
        "my_events_list.jinja2",
        events=prepared_events,
        page=page + 1,
        total_pages=total_pages,
        total_events=len(events),
    )
    await _safe_edit_text(callback, text, _my_events_list_keyboard(page, total_pages))
    await callback.answer()


async def send_my_event_card(message: Message, user_id: int, event_id: int, page: int = 0) -> bool:
    events = await _get_my_events(user_id)
    if events is None:
        await message.answer("Не удалось получить мероприятия.")
        return False
    event = next((item for item in events if int(item.get("id", -1)) == event_id), None)
    if not event:
        await message.answer("Мероприятие не найдено или уже удалено.")
        return False
    await message.answer(
        render("my_event.jinja2", event=event),
        reply_markup=_my_event_actions_keyboard(event_id=event_id, page=page),
    )
    return True


async def _show_my_event_card(callback: CallbackQuery, event_id: int, page: int) -> None:
    events = await _get_my_events(callback.from_user.id)
    if events is None:
        await callback.answer("Не удалось получить мероприятия", show_alert=True)
        return
    event = next((item for item in events if int(item.get("id", -1)) == event_id), None)
    if not event:
        await callback.answer("Мероприятие не найдено", show_alert=True)
        return

    await _safe_edit_text(
        callback,
        render("my_event.jinja2", event=event),
        _my_event_actions_keyboard(event_id=event_id, page=page),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "my_events")
async def get_my_events(callback: CallbackQuery) -> None:
    await _show_my_events_page(callback, page=0)


@router.callback_query(lambda c: c.data.startswith("my_events_page_"))
async def paginate_my_events(callback: CallbackQuery) -> None:
    try:
        page = int(callback.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректная страница", show_alert=True)
        return
    await _show_my_events_page(callback, page=page)


def _parse_event_and_page(data: str, prefix: str) -> tuple[int, int] | None:
    if not data.startswith(prefix):
        return None
    try:
        tail = data[len(prefix):]
        event_id_raw, page_raw = tail.split("_", 1)
        return int(event_id_raw), int(page_raw)
    except (ValueError, IndexError):
        return None


@router.callback_query(lambda c: c.data.startswith("my_event_edit_") and not c.data.startswith("my_event_edit_field_"))
async def start_edit_event(callback: CallbackQuery, state: FSMContext) -> None:
    parsed = _parse_event_and_page(callback.data, "my_event_edit_")
    if not parsed:
        await callback.answer("Некорректный идентификатор мероприятия", show_alert=True)
        return
    event_id, page = parsed
    await state.set_state(EditEventState.choosing_field)
    await state.update_data(
        edit_event_id=event_id,
        edit_event_page=page,
        edit_event_updates={},
        edit_event_changed_fields={},
    )
    await callback.message.answer(
        "Что вы хотите отредактировать?",
        reply_markup=_edit_fields_keyboard(event_id=event_id, page=page),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("my_event_edit_field_"))
async def choose_edit_event_field(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    data = await state.get_data()
    stored_event_id = data.get("edit_event_id")
    stored_page = data.get("edit_event_page")
    if stored_event_id is None or stored_page is None:
        await callback.message.answer("Сессия редактирования устарела. Откройте мероприятие снова.")
        await state.clear()
        return
    try:
        payload = callback.data.replace("my_event_edit_field_", "", 1)
        field_key, event_id_raw, page_raw = payload.rsplit("_", 2)
        event_id = int(event_id_raw)
        page = int(page_raw)
    except (ValueError, IndexError):
        await callback.message.answer("Некорректное поле редактирования.")
        return
    if event_id != stored_event_id or page != stored_page or field_key not in EDITABLE_EVENT_FIELDS:
        await callback.message.answer("Некорректное поле редактирования.")
        return
    await state.set_state(EditEventState.waiting_new_value)
    await state.update_data(edit_event_field=field_key)
    await callback.message.answer(FIELD_VALUE_PROMPTS.get(field_key, "Введите новое значение:"))


@router.message(EditEventState.waiting_new_value)
async def update_event_field_value(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    field_key = data.get("edit_event_field")
    if not field_key:
        await message.answer("Поле для редактирования не выбрано.")
        await state.clear()
        return
    parsed = _parse_event_field_update(field_key, message.text or "")
    if not parsed:
        await message.answer("Некорректное значение. Попробуйте снова в нужном формате.")
        return
    update_payload, shown_value = parsed
    updates = dict(data.get("edit_event_updates", {}))
    updates.update(update_payload)
    changed_fields = dict(data.get("edit_event_changed_fields", {}))
    changed_fields[field_key] = shown_value
    await state.update_data(
        edit_event_updates=updates,
        edit_event_changed_fields=changed_fields,
    )
    await state.set_state(EditEventState.confirm_more)
    await message.answer("Хотите изменить что-то еще?", reply_markup=_edit_more_keyboard())


@router.callback_query(lambda c: c.data in {"my_event_more_yes", "my_event_more_no"})
async def confirm_more_edit_event(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    event_id = data.get("edit_event_id")
    page = data.get("edit_event_page")
    updates = dict(data.get("edit_event_updates", {}))
    changed_fields = dict(data.get("edit_event_changed_fields", {}))
    if event_id is None or page is None or not updates:
        await callback.message.answer("Нет данных для сохранения. Попробуйте снова.")
        await state.clear()
        await callback.answer()
        return
    if callback.data == "my_event_more_yes":
        await state.set_state(EditEventState.choosing_field)
        await callback.message.answer(
            "Что вы хотите отредактировать?",
            reply_markup=_edit_fields_keyboard(event_id=event_id, page=page),
        )
        await callback.answer()
        return

    response = await _request_to_consumer(
        callback.from_user.id,
        "update_event",
        {"event_id": event_id, "updates": updates},
    )
    if not response or "error" in response:
        await callback.message.answer("Не удалось обновить мероприятие. Попробуйте позже.")
        await callback.answer()
        return

    event_title = response.get("old_event_title") or response.get("event_title") or "мероприятие"
    volunteer_ids = response.get("volunteer_telegram_ids") or []
    for volunteer_id in volunteer_ids:
        lines = [f"В мероприятие «{event_title}» внесены изменения."]
        for field, value in changed_fields.items():
            lines.append(_notification_field_text(field, value))
        try:
            await callback.bot.send_message(volunteer_id, "\n".join(lines))
        except TelegramBadRequest:
            pass

    await callback.message.answer("Изменения сохранены.")
    await state.clear()
    await _show_my_event_card(callback, event_id=event_id, page=page)


@router.callback_query(lambda c: c.data.startswith("my_event_participants_"))
async def get_event_participants(callback: CallbackQuery) -> None:
    parsed = _parse_event_and_page(callback.data, "my_event_participants_")
    if not parsed:
        await callback.answer("Некорректный идентификатор мероприятия", show_alert=True)
        return
    event_id, page = parsed

    events = await _get_my_events(callback.from_user.id)
    if events is None:
        await callback.answer("Не удалось получить мероприятия", show_alert=True)
        return
    event = next((item for item in events if int(item.get("id", -1)) == event_id), None)
    if not event:
        await callback.answer("Мероприятие не найдено", show_alert=True)
        return

    participants_response = await _request_to_consumer(
        callback.from_user.id,
        "get_event_participants",
        {"event_id": event_id},
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
    await callback.message.answer("\n".join(lines), reply_markup=_my_event_actions_keyboard(event_id, page))
    await callback.answer()


def _delete_confirm_keyboard(event_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data=f"my_event_delete_yes_{event_id}_{page}"),
                InlineKeyboardButton(text="Нет", callback_data=f"my_event_delete_no_{event_id}_{page}"),
            ]
        ]
    )


@router.callback_query(
    lambda c: c.data.startswith("my_event_delete_")
    and not c.data.startswith("my_event_delete_yes_")
    and not c.data.startswith("my_event_delete_no_")
)
async def start_delete_event(callback: CallbackQuery) -> None:
    parsed = _parse_event_and_page(callback.data, "my_event_delete_")
    if not parsed:
        await callback.answer("Некорректный идентификатор мероприятия", show_alert=True)
        return
    event_id, page = parsed
    await callback.message.answer(
        "Точно хотите удалить мероприятие?",
        reply_markup=_delete_confirm_keyboard(event_id, page),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("my_event_delete_no_"))
async def cancel_delete_event(callback: CallbackQuery) -> None:
    parsed = _parse_event_and_page(callback.data, "my_event_delete_no_")
    if not parsed:
        await callback.answer("Некорректный идентификатор мероприятия", show_alert=True)
        return
    event_id, page = parsed
    await _show_my_event_card(callback, event_id=event_id, page=page)


@router.callback_query(lambda c: c.data.startswith("my_event_delete_yes_"))
async def confirm_delete_event(callback: CallbackQuery, state: FSMContext) -> None:
    parsed = _parse_event_and_page(callback.data, "my_event_delete_yes_")
    if not parsed:
        await callback.answer("Некорректный идентификатор мероприятия", show_alert=True)
        return
    event_id, _ = parsed
    events = await _get_my_events(callback.from_user.id)
    if events is None:
        await callback.answer("Не удалось получить мероприятия", show_alert=True)
        return
    event = next((item for item in events if int(item.get("id", -1)) == event_id), None)
    if not event:
        await callback.answer("Мероприятие не найдено", show_alert=True)
        return
    await state.set_state(DeleteEventState.waiting_reason)
    await state.update_data(delete_event_id=event_id, delete_event_title=event.get("title"))
    await callback.message.answer("Если мероприятие еще не прошло, укажи причину отмены.")
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
