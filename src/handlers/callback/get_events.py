import asyncio
from datetime import datetime

import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config.settings import settings
from src.handlers.callback.router import router
from src.handlers.state.event_filters import EventFilterState
from src.storage.rabbit import channel_pool
from src.templates.env import render

FILTER_DIRECTIONS = [
    "Здравоохранение",
    "ЧС",
    "Ветераны",
    "Дети и молодежь",
    "Спорт",
    "Животные",
    "Старшее поколение",
    "Люди с ОВЗ",
    "Экология",
    "Культура и искусство",
    "Поиск пропавших",
    "Образование",
]


def _empty_filters() -> dict:
    return {"cities": [], "directions": [], "date_from": "", "date_to": ""}


async def _safe_edit_message(
    message: Message, text: str, reply_markup: InlineKeyboardMarkup | None = None
) -> None:
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc):
            raise


async def _get_filters(state: FSMContext) -> tuple[dict, dict]:
    data = await state.get_data()
    applied = data.get("event_filters_applied") or _empty_filters()
    draft = data.get("event_filters_draft") or {
        "cities": list(applied.get("cities", [])),
        "directions": list(applied.get("directions", [])),
        "date_from": applied.get("date_from", ""),
        "date_to": applied.get("date_to", ""),
    }
    await state.update_data(event_filters_applied=applied, event_filters_draft=draft)
    return applied, draft


async def _request_to_consumer(
    user_id: int, action: str, payload: dict | None = None
) -> dict | None:
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

        for _ in range(10):
            try:
                res = await user_queue.get(timeout=3)
                await res.ack()
                return msgpack.unpackb(res.body)
            except QueueEmpty:
                await asyncio.sleep(1)
    return None


def _events_keyboard(index: int, total: int) -> InlineKeyboardMarkup:
    prev_index = (index - 1) % total
    next_index = (index + 1) % total
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Фильтры", callback_data="event_filters_open")],
            [
                InlineKeyboardButton(text="⬅️", callback_data=f"events_{prev_index}"),
                InlineKeyboardButton(text="Участвовать", callback_data=f"participate_{index}"),
                InlineKeyboardButton(text="➡️", callback_data=f"events_{next_index}"),
            ],
        ]
    )


def _filters_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="event_filters_back_to_list")],
            [InlineKeyboardButton(text="Город", callback_data="event_filter_city_start")],
            [InlineKeyboardButton(text="Направление", callback_data="event_filter_direction_page_0")],
            [InlineKeyboardButton(text="Дата", callback_data="event_filter_date_start")],
            [
                InlineKeyboardButton(text="Применить фильтры", callback_data="event_filters_apply"),
                InlineKeyboardButton(text="Сбросить фильтры", callback_data="event_filters_reset"),
            ],
        ]
    )


def _direction_keyboard(page: int) -> InlineKeyboardMarkup:
    page_size = 4
    pages = max(1, (len(FILTER_DIRECTIONS) + page_size - 1) // page_size)
    page = page % pages
    start = page * page_size
    chunk = FILTER_DIRECTIONS[start:start + page_size]

    rows = []
    for i in range(0, len(chunk), 2):
        row = []
        for item in chunk[i:i + 2]:
            idx = FILTER_DIRECTIONS.index(item)
            row.append(InlineKeyboardButton(text=item, callback_data=f"event_filter_direction_pick_{idx}"))
        rows.append(row)

    prev_page = (page - 1) % pages
    next_page = (page + 1) % pages
    rows.append(
        [
            InlineKeyboardButton(text="⬅️", callback_data=f"event_filter_direction_page_{prev_page}"),
            InlineKeyboardButton(text="Назад", callback_data="event_filters_open"),
            InlineKeyboardButton(text="➡️", callback_data=f"event_filter_direction_page_{next_page}"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _ask_more_city_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="event_filter_city_more_yes"),
                InlineKeyboardButton(text="Нет", callback_data="event_filter_city_more_no"),
            ]
        ]
    )


def _ask_more_direction_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="event_filter_direction_more_yes"),
                InlineKeyboardButton(text="Нет", callback_data="event_filter_direction_more_no"),
            ]
        ]
    )


async def _show_event_by_index(callback: CallbackQuery, state: FSMContext, index: int) -> None:
    applied, _ = await _get_filters(state)
    response = await _request_to_consumer(
        callback.from_user.id, "get_events", {"filters": applied}
    )
    if not response or "error" in response:
        await callback.answer("Не удалось получить список мероприятий", show_alert=True)
        return

    events = response.get("events", [])
    if not events:
        await _safe_edit_message(callback.message, "Ничего не найдено")
        await callback.answer()
        return

    index = index % len(events)
    event = events[index]
    keyboard = _events_keyboard(index=index, total=len(events))
    await _safe_edit_message(
        callback.message, render("event.jinja2", event=event), reply_markup=keyboard
    )
    await callback.answer()


async def _show_filters(callback: CallbackQuery) -> None:
    await _safe_edit_message(callback.message, "Выберите фильтры:", reply_markup=_filters_keyboard())


@router.callback_query(lambda c: c.data == "events")
async def get_events(callback: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(event_filters_applied=_empty_filters(), event_filters_draft=_empty_filters())
    await _show_event_by_index(callback, state, index=0)


@router.callback_query(lambda c: c.data.startswith("events_"))
async def paginate_events(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        index = int(callback.data.split("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный индекс", show_alert=True)
        return
    await _show_event_by_index(callback, state, index=index)


@router.callback_query(lambda c: c.data == "event_filters_open")
async def open_filters(callback: CallbackQuery, state: FSMContext) -> None:
    await _get_filters(state)
    await _show_filters(callback)
    await callback.answer()


@router.callback_query(lambda c: c.data == "event_filters_back_to_list")
async def filters_back_to_list(callback: CallbackQuery, state: FSMContext) -> None:
    await _show_event_by_index(callback, state, index=0)


@router.callback_query(lambda c: c.data == "event_filter_city_start")
async def filter_city_start(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(EventFilterState.city_input)
    await callback.message.answer("Введите название города:")
    await callback.answer()


@router.message(EventFilterState.city_input)
async def filter_city_input(message: Message, state: FSMContext) -> None:
    city = (message.text or "").strip()
    if not city:
        await message.answer("Город не должен быть пустым. Введи снова.")
        return
    _, draft = await _get_filters(state)
    if city not in draft["cities"]:
        draft["cities"].append(city)
        await state.update_data(event_filters_draft=draft)
    await message.answer("Хотите еще выбрать город?", reply_markup=_ask_more_city_keyboard())


@router.callback_query(lambda c: c.data == "event_filter_city_more_yes")
async def filter_city_more_yes(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(EventFilterState.city_input)
    await callback.message.answer("Введите название города:")
    await callback.answer()


@router.callback_query(lambda c: c.data == "event_filter_city_more_no")
async def filter_city_more_no(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(None)
    await _show_filters(callback)
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("event_filter_direction_page_"))
async def filter_direction_page(callback: CallbackQuery) -> None:
    try:
        page = int(callback.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректная страница", show_alert=True)
        return
    await _safe_edit_message(
        callback.message, "Выберите направление:", reply_markup=_direction_keyboard(page)
    )
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("event_filter_direction_pick_"))
async def filter_direction_pick(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        idx = int(callback.data.rsplit("_", 1)[1])
        direction = FILTER_DIRECTIONS[idx]
    except (ValueError, IndexError):
        await callback.answer("Некорректное направление", show_alert=True)
        return
    _, draft = await _get_filters(state)
    if direction not in draft["directions"]:
        draft["directions"].append(direction)
        await state.update_data(event_filters_draft=draft)
    await callback.message.answer(
        "Хотите еще выбрать направление?", reply_markup=_ask_more_direction_keyboard()
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "event_filter_direction_more_yes")
async def filter_direction_more_yes(callback: CallbackQuery) -> None:
    await _safe_edit_message(
        callback.message, "Выберите направление:", reply_markup=_direction_keyboard(0)
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "event_filter_direction_more_no")
async def filter_direction_more_no(callback: CallbackQuery) -> None:
    await _show_filters(callback)
    await callback.answer()


@router.callback_query(lambda c: c.data == "event_filter_date_start")
async def filter_date_start(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(EventFilterState.date_input)
    await callback.message.answer(
        "Введите дату в формате ДД.ММ.ГГГГ или диапазон ДД.ММ.ГГГГ - ДД.ММ.ГГГГ"
    )
    await callback.answer()


@router.message(EventFilterState.date_input)
async def filter_date_input(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if " - " in text:
        left, right = [part.strip() for part in text.split(" - ", 1)]
        try:
            datetime.strptime(left, "%d.%m.%Y")
            datetime.strptime(right, "%d.%m.%Y")
        except ValueError:
            await message.answer("Неверный формат диапазона. Пример: 01.05.2026 - 10.05.2026")
            return
        _, draft = await _get_filters(state)
        draft["date_from"] = left
        draft["date_to"] = right
        await state.update_data(event_filters_draft=draft)
    else:
        try:
            datetime.strptime(text, "%d.%m.%Y")
        except ValueError:
            await message.answer("Неверный формат даты. Пример: 01.05.2026")
            return
        _, draft = await _get_filters(state)
        draft["date_from"] = text
        draft["date_to"] = text
        await state.update_data(event_filters_draft=draft)

    await state.set_state(None)
    await message.answer("Дата фильтра сохранена.")
    await message.answer("Выберите фильтры:", reply_markup=_filters_keyboard())


@router.callback_query(lambda c: c.data == "event_filters_apply")
async def apply_filters(callback: CallbackQuery, state: FSMContext) -> None:
    _, draft = await _get_filters(state)
    applied = {
        "cities": list(draft.get("cities", [])),
        "directions": list(draft.get("directions", [])),
        "date_from": draft.get("date_from", ""),
        "date_to": draft.get("date_to", ""),
    }
    await state.update_data(event_filters_applied=applied)
    await _show_event_by_index(callback, state, index=0)


@router.callback_query(lambda c: c.data == "event_filters_reset")
async def reset_filters(callback: CallbackQuery, state: FSMContext) -> None:
    empty = _empty_filters()
    await state.update_data(event_filters_applied=empty, event_filters_draft=empty)
    await _show_event_by_index(callback, state, index=0)


@router.callback_query(lambda c: c.data.startswith("participate_"))
async def participate_event(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        index = int(callback.data.split("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный индекс", show_alert=True)
        return

    applied, _ = await _get_filters(state)
    events_response = await _request_to_consumer(
        callback.from_user.id, "get_events", {"filters": applied}
    )
    if not events_response or "error" in events_response:
        await callback.answer("Не удалось получить мероприятия", show_alert=True)
        return
    events = events_response.get("events", [])
    if not events:
        await callback.answer("Мероприятия не найдены", show_alert=True)
        return

    event = events[index % len(events)]
    response = await _request_to_consumer(
        callback.from_user.id, "participate_event", {"event_id": event.get("id")}
    )
    if not response or "error" in response:
        if response and response.get("error") == "already_participating":
            await callback.answer("Вы уже участвуете в этом мероприятии", show_alert=True)
            return
        await callback.answer("Не удалось записаться на мероприятие", show_alert=True)
        return

    volunteer = response.get("volunteer") or {}
    participation_id = response.get("participation_id")
    organizer_tg = response.get("organizer_telegram_id")
    event_title = response.get("event_title") or event.get("title")
    if organizer_tg and participation_id:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Принять заявку",
                        callback_data=f"participation_approve_{participation_id}",
                    ),
                    InlineKeyboardButton(
                        text="Отклонить заявку",
                        callback_data=f"participation_reject_{participation_id}",
                    ),
                ]
            ]
        )
        try:
            await callback.bot.send_message(organizer_tg, "У вас новая заявка на участие")
            await callback.bot.send_message(
                organizer_tg,
                (
                    "<b>Профиль</b>\n\n"
                    f"👤 Имя: {volunteer.get('name')}\n"
                    f"🎂 Возраст: {volunteer.get('age')}\n"
                    f"📍 Город: {volunteer.get('city')}\n"
                    f"📞 Телефон: {volunteer.get('phone')}\n"
                    f"⚧ Пол: {'Ж' if volunteer.get('gender') == 'f' else 'М'}\n"
                    f"🎯 Мероприятие: {event_title}"
                ),
                reply_markup=keyboard,
            )
            await callback.answer("Заявка на участие отправлена", show_alert=True)
            return
        except TelegramBadRequest:
            await callback.answer(
                "Заявка создана, но организатору не удалось отправить сообщение. "
                "Пусть организатор напишет боту /start.",
                show_alert=True,
            )
            return

    await callback.answer("Заявка на участие отправлена", show_alert=True)
