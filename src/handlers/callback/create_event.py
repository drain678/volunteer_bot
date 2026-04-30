import asyncio
from datetime import datetime
import logging
import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config.settings import settings
from src.handlers.callback.router import router
from src.handlers.state.create_event import CreateEventState
from aiogram.exceptions import TelegramBadRequest
from src.storage.rabbit import channel_pool
from src.templates.env import render
from consumer.logger import LOGGING_CONFIG, logger

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


async def _safe_edit_message(
    message: Message, text: str, reply_markup: InlineKeyboardMarkup | None = None
) -> None:
    await message.edit_text(text, reply_markup=reply_markup)


def _direction_more_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="event_direction_more_yes"),
                InlineKeyboardButton(text="Нет", callback_data="event_direction_more_no"),
            ]
        ]
    )


def _directions_keyboard(
    directions: list[str], page: int, show_cancel: bool = False
) -> InlineKeyboardMarkup:
    page_size = 4
    pages = max(1, (len(directions) + page_size - 1) // page_size)
    page = page % pages
    start = page * page_size
    chunk = directions[start:start + page_size]

    rows = []
    for i in range(0, len(chunk), 2):
        row = []
        for item in chunk[i:i + 2]:
            idx = directions.index(item)
            row.append(
                InlineKeyboardButton(
                    text=item,
                    callback_data=f"event_direction_pick_{idx}",
                )
            )
        rows.append(row)

    prev_page = (page - 1) % pages
    next_page = (page + 1) % pages
    controls = [
        InlineKeyboardButton(text="⬅️", callback_data=f"event_direction_page_{prev_page}")
    ]
    if show_cancel:
        controls.append(
            InlineKeyboardButton(text="Отменить выбор", callback_data="event_direction_cancel")
        )
    controls.append(
        InlineKeyboardButton(text="➡️", callback_data=f"event_direction_page_{next_page}")
    )
    rows.append(controls)
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _request_to_consumer(user_id: int, action: str, payload: dict | None = None) -> dict | None:
    body = {"action": action, "id": user_id}
    if payload:
        body.update(payload)

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


@router.callback_query(lambda c: c.data == "create_event")
async def create_event_start(callback: CallbackQuery, state: FSMContext) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("НАЧАЛО СОЗДАНИЕ МЕРОПРИЯТИЯ", extra={"body": callback.from_user.id})
    await state.set_state(CreateEventState.title)
    await callback.message.answer("Название мероприятия?")
    await callback.answer()


@router.message(CreateEventState.title)
async def event_title(message: Message, state: FSMContext) -> None:
    title = (message.text or "").strip()
    if not title:
        await message.answer("Название не должно быть пустым. Введи снова.")
        return
    await state.update_data(title=title)
    await state.set_state(CreateEventState.description)
    await message.answer("Описание мероприятия?")


@router.message(CreateEventState.description)
async def event_description(message: Message, state: FSMContext) -> None:
    description = (message.text or "").strip()
    if not description:
        await message.answer("Описание не должно быть пустым. Введи снова.")
        return
    await state.update_data(description=description)
    await state.set_state(CreateEventState.min_age)
    await message.answer("Минимальный возраст участников?")


@router.message(CreateEventState.min_age)
async def event_min_age(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Минимальный возраст должен быть числом.")
        return
    min_age = int(text)
    if min_age < 14 or min_age > 100:
        await message.answer("Укажи возраст в диапазоне 14-100.")
        return
    await state.update_data(min_age=min_age)
    await state.set_state(CreateEventState.city)
    await message.answer("Город проведения?")


@router.message(CreateEventState.city)
async def event_city(message: Message, state: FSMContext) -> None:
    city = (message.text or "").strip()
    if not city:
        await message.answer("Город не должен быть пустым. Введи снова.")
        return
    await state.update_data(city=city)
    await state.set_state(CreateEventState.start_time)
    await message.answer("Дата и время начала? Формат: ДД.ММ.ГГГГ ЧЧ:ММ")


@router.message(CreateEventState.start_time)
async def event_start_time(message: Message, state: FSMContext) -> None:
    start_time = (message.text or "").strip()
    try:
        dt = datetime.strptime(start_time, "%d.%m.%Y %H:%M")
    except ValueError:
        await message.answer("Неверный формат. Пример: 30.04.2026 14:30")
        return
    if dt < datetime.now():
        await message.answer("Дата и время не могут быть раньше текущего момента.")
        return
    await state.update_data(start_time=start_time)
    await state.set_state(CreateEventState.duration_hours)
    await message.answer("Длительность в часах? Пример: 2 или 2.5")


@router.message(CreateEventState.duration_hours)
async def event_duration(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip().replace(",", ".")
    try:
        duration_hours = float(text)
    except ValueError:
        await message.answer("Длительность должна быть числом. Пример: 2.5")
        return
    if duration_hours <= 0:
        await message.answer("Длительность должна быть больше 0.")
        return
    await state.update_data(duration_hours=duration_hours)
    await state.set_state(CreateEventState.category)
    await state.update_data(event_directions=FILTER_DIRECTIONS, selected_directions=[])
    await message.answer(
        "Выберите направление:",
        reply_markup=_directions_keyboard(FILTER_DIRECTIONS, page=0, show_cancel=False),
    )


@router.callback_query(
    StateFilter(CreateEventState.category),
    lambda c: c.data.startswith("event_direction_page_"),
)
async def event_direction_page(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    directions = data.get("event_directions", [])
    selected_directions = list(data.get("selected_directions", []))
    if not directions:
        await callback.answer("Направления мероприятий не найдены", show_alert=True)
        return
    try:
        page = int(callback.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректная страница", show_alert=True)
        return
    await callback.message.edit_reply_markup(
        reply_markup=_directions_keyboard(
            directions, page=page, show_cancel=bool(selected_directions)
        )
    )
    await callback.answer()


@router.callback_query(
    StateFilter(CreateEventState.category),
    lambda c: c.data.startswith("event_direction_pick_"),
)
async def event_direction_pick(callback: CallbackQuery, state: FSMContext) -> None:
    event_data = await state.get_data()
    directions = event_data.get("event_directions", [])
    try:
        idx = int(callback.data.rsplit("_", 1)[1])
        direction = directions[idx]
    except (ValueError, IndexError, TypeError):
        await callback.answer("Некорректное направление", show_alert=True)
        return

    selected_directions = list(event_data.get("selected_directions", []))
    if direction not in selected_directions:
        selected_directions.append(direction)
    await state.update_data(selected_directions=selected_directions)
    await state.set_state(CreateEventState.direction_more)
    await callback.message.answer(
        "Хотите еще выбрать направление?",
        reply_markup=_direction_more_keyboard(),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(CreateEventState.category),
    lambda c: c.data == "event_direction_cancel",
)
async def event_direction_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    event_data = await state.get_data()
    selected_directions = list(event_data.get("selected_directions", []))
    if not selected_directions:
        await callback.answer("Сначала выберите хотя бы одно направление", show_alert=True)
        return
    await state.set_state(CreateEventState.direction_more)
    await _safe_edit_message(
        callback.message,
        "Хотите еще выбрать направление?",
        reply_markup=_direction_more_keyboard(),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(CreateEventState.direction_more),
    lambda c: c.data == "event_direction_more_yes",
)
async def event_direction_more_yes(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(CreateEventState.category)
    await _safe_edit_message(
        callback.message,
        "Выберите направление:",
        reply_markup=_directions_keyboard(FILTER_DIRECTIONS, page=0, show_cancel=True),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(CreateEventState.direction_more),
    lambda c: c.data == "event_direction_more_no",
)
async def event_direction_more_no(callback: CallbackQuery, state: FSMContext) -> None:
    event_data = await state.get_data()
    selected_directions = list(event_data.get("selected_directions", []))
    if not selected_directions:
        await callback.answer("Нужно выбрать хотя бы одно направление", show_alert=True)
        return
    direction_value = ", ".join(selected_directions)

    create_response = await _request_to_consumer(
        callback.from_user.id,
        "create_event",
        {
            "title": event_data.get("title"),
            "description": event_data.get("description"),
            "min_age": event_data.get("min_age"),
            "city": event_data.get("city"),
            "start_time": event_data.get("start_time"),
            "duration_hours": event_data.get("duration_hours"),
            "direction": direction_value,
        },
    )
    if not create_response or "error" in create_response:
        await callback.message.answer("Не удалось создать мероприятие. Попробуй позже.")
        await callback.answer()
        return

    await callback.message.answer("Мероприятие успешно создано!")
    await callback.message.answer(render("my_event.jinja2", event=create_response))
    notify_volunteer_ids = create_response.get("notify_volunteer_ids") or []
    event_id = create_response.get("id")
    if event_id:
        notify_keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Участвовать", callback_data=f"participate_event_{event_id}"
                    )
                ]
            ]
        )
        for volunteer_id in notify_volunteer_ids:
            try:
                await callback.bot.send_message(volunteer_id, "Появилось новое мероприятие")
                await callback.bot.send_message(
                    volunteer_id,
                    render("event.jinja2", event=create_response),
                    reply_markup=notify_keyboard,
                )
            except TelegramBadRequest:
                continue
    await state.clear()
    await callback.answer()
