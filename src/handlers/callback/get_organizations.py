import asyncio
import logging

import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from consumer.logger import LOGGING_CONFIG, logger

from config.settings import settings
from src.handlers.callback.get_events import _events_keyboard
from src.handlers.callback.get_events import _request_to_consumer as _request_events_to_consumer
from src.handlers.callback.get_events import _empty_filters as _empty_event_filters
from src.handlers.callback.router import router
from src.handlers.state.organization_filters import OrganizationFilterState
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

FILTER_TYPES = [
    "ВУЗ",
    "Гос. учреждение",
    "Коммерч-ая организация",
    "НКО",
    "Общественное объединение",
    "Орган власти",
    "ССУЗ",
    "Школа",
]


def _empty_filters() -> dict:
    return {"cities": [], "directions": [], "types": []}


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
    applied = data.get("organization_filters_applied") or _empty_filters()
    draft = data.get("organization_filters_draft") or {
        "cities": list(applied.get("cities", [])),
        "directions": list(applied.get("directions", [])),
        "types": list(applied.get("types", [])),
    }
    await state.update_data(
        organization_filters_applied=applied,
        organization_filters_draft=draft,
    )
    return applied, draft


async def _request_to_consumer(user_id: int, filters: dict | None = None) -> dict | None:
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
        if filters:
            body["filters"] = filters
        await exchange.publish(
            aio_pika.Message(msgpack.packb(body)),
            routing_key="user_messages",
        )
        logger.info("ОТПРАВИЛИ ЗАПРОС НА СПИСОК ОРГАНИЗАЦИЙ В БД", extra={"body": user_id})

        for _ in range(10):
            try:
                res = await user_queue.get(timeout=3)
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
            [InlineKeyboardButton(text="Фильтры", callback_data="org_filters_open")],
            [
                InlineKeyboardButton(text="⬅️", callback_data=f"organizations_{prev_index}"),
                InlineKeyboardButton(text="Мероприятия", callback_data=f"organization_events_{index}"),
                InlineKeyboardButton(text="➡️", callback_data=f"organizations_{next_index}"),
            ],
        ]
    )


def _filters_keyboard() -> InlineKeyboardMarkup:
    logger.info("ОТПРАВИЛИ КЛАВИАТУРУ ФИЛЬТРОВ")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="org_filters_back_to_list")],
            [InlineKeyboardButton(text="Город", callback_data="org_filter_city_start")],
            [InlineKeyboardButton(text="Направление", callback_data="org_filter_direction_page_0")],
            [InlineKeyboardButton(text="Тип организации", callback_data="org_filter_type_page_0")],
            [
                InlineKeyboardButton(text="Применить фильтры", callback_data="org_filters_apply"),
                InlineKeyboardButton(text="Сбросить фильтры", callback_data="org_filters_reset"),
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
            row.append(InlineKeyboardButton(text=item, callback_data=f"org_filter_direction_pick_{idx}"))
        rows.append(row)

    prev_page = (page - 1) % pages
    next_page = (page + 1) % pages
    rows.append(
        [
            InlineKeyboardButton(text="⬅️", callback_data=f"org_filter_direction_page_{prev_page}"),
            InlineKeyboardButton(text="Назад", callback_data="org_filters_open"),
            InlineKeyboardButton(text="➡️", callback_data=f"org_filter_direction_page_{next_page}"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _type_keyboard(page: int) -> InlineKeyboardMarkup:
    page_size = 4
    pages = max(1, (len(FILTER_TYPES) + page_size - 1) // page_size)
    page = page % pages
    start = page * page_size
    chunk = FILTER_TYPES[start:start + page_size]

    rows = []
    for item in chunk:
        idx = FILTER_TYPES.index(item)
        rows.append(
            [InlineKeyboardButton(text=item, callback_data=f"org_filter_type_pick_{idx}")]
        )

    prev_page = (page - 1) % pages
    next_page = (page + 1) % pages
    rows.append(
        [
            InlineKeyboardButton(text="⬅️", callback_data=f"org_filter_type_page_{prev_page}"),
            InlineKeyboardButton(text="Назад", callback_data="org_filters_open"),
            InlineKeyboardButton(text="➡️", callback_data=f"org_filter_type_page_{next_page}"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _ask_more_city_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="org_filter_city_more_yes"),
                InlineKeyboardButton(text="Нет", callback_data="org_filter_city_more_no"),
            ]
        ]
    )


def _ask_more_direction_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="org_filter_direction_more_yes"),
                InlineKeyboardButton(text="Нет", callback_data="org_filter_direction_more_no"),
            ]
        ]
    )


def _ask_more_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="org_filter_type_more_yes"),
                InlineKeyboardButton(text="Нет", callback_data="org_filter_type_more_no"),
            ]
        ]
    )


async def _show_organization_by_index(callback: CallbackQuery, state: FSMContext, index: int) -> None:
    applied, _ = await _get_filters(state)
    response = await _request_to_consumer(callback.from_user.id, applied)
    if not response or "error" in response:
        await callback.answer("Не удалось получить список организаций", show_alert=True)
        return

    organizations = response.get("organizations", [])
    if not organizations:
        await _safe_edit_message(callback.message, "Ничего не найдено")
        await callback.answer()
        return

    index = index % len(organizations)
    organization = organizations[index]
    keyboard = _organizations_keyboard(index=index, total=len(organizations))

    await _safe_edit_message(
        callback.message,
        render("organization.jinja2", user=organization),
        reply_markup=keyboard,
    )
    await callback.answer()


async def _show_filters(message: Message | CallbackQuery) -> None:
    target = message.message if isinstance(message, CallbackQuery) else message
    if isinstance(message, CallbackQuery):
        await _safe_edit_message(
            target, "Выберите фильтры:", reply_markup=_filters_keyboard()
        )
    else:
        await target.answer("Выберите фильтры:", reply_markup=_filters_keyboard())


@router.callback_query(lambda c: c.data == "organizations")
async def get_organizations(callback: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(
        organization_filters_applied=_empty_filters(),
        organization_filters_draft=_empty_filters(),
    )
    await _show_organization_by_index(callback, state, index=0)


@router.callback_query(lambda c: c.data.startswith("organizations_"))
async def paginate_organizations(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        index = int(callback.data.split("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный индекс", show_alert=True)
        return
    await _show_organization_by_index(callback, state, index=index)


@router.callback_query(lambda c: c.data.startswith("organization_events_"))
async def organization_events(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        index = int(callback.data.split("_", 2)[2])
    except (ValueError, IndexError):
        await callback.answer("Некорректный индекс", show_alert=True)
        return

    applied, _ = await _get_filters(state)
    response = await _request_to_consumer(callback.from_user.id, applied)
    if not response or "error" in response:
        await callback.answer("Не удалось получить список организаций", show_alert=True)
        return
    organizations = response.get("organizations", [])
    if not organizations:
        await _safe_edit_message(callback.message, "Ничего не найдено")
        await callback.answer()
        return

    organization = organizations[index % len(organizations)]
    organization_id = organization.get("id")
    organization_name = organization.get("organization_name") or "эта организация"
    if not organization_id:
        await callback.answer("Не удалось открыть мероприятия организации", show_alert=True)
        return

    await state.update_data(
        event_filters_base={"organization_id": organization_id},
        event_filters_applied=_empty_event_filters(),
        event_filters_draft=_empty_event_filters(),
    )
    events_response = await _request_events_to_consumer(
        callback.from_user.id,
        "get_events",
        {"filters": {"organization_id": organization_id}},
    )
    events = [] if not events_response or "error" in events_response else events_response.get("events", [])
    if not events:
        await callback.message.answer(
            f"У организации {organization_name} пока нет мероприятий."
        )
        await callback.answer()
        return
    first_event = events[0]
    await callback.message.answer(
        render("event.jinja2", event=first_event),
        reply_markup=_events_keyboard(index=0, total=len(events)),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "org_filters_open")
async def open_filters(callback: CallbackQuery, state: FSMContext) -> None:
    await _get_filters(state)
    await _show_filters(callback)
    await callback.answer()


@router.callback_query(lambda c: c.data == "org_filters_back_to_list")
async def filters_back_to_list(callback: CallbackQuery, state: FSMContext) -> None:
    await _show_organization_by_index(callback, state, index=0)


@router.callback_query(lambda c: c.data == "org_filter_city_start")
async def filter_city_start(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(OrganizationFilterState.city_input)
    await callback.message.answer("Введите название города:")
    await callback.answer()


@router.message(OrganizationFilterState.city_input)
async def filter_city_input(message: Message, state: FSMContext) -> None:
    city = (message.text or "").strip()
    if not city:
        await message.answer("Город не должен быть пустым. Введи снова.")
        return
    _, draft = await _get_filters(state)
    if city not in draft["cities"]:
        draft["cities"].append(city)
        await state.update_data(organization_filters_draft=draft)
    await message.answer("Хотите еще выбрать город?", reply_markup=_ask_more_city_keyboard())


@router.callback_query(lambda c: c.data == "org_filter_city_more_yes")
async def filter_city_more_yes(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(OrganizationFilterState.city_input)
    await callback.message.answer("Введите название города:")
    await callback.answer()


@router.callback_query(lambda c: c.data == "org_filter_city_more_no")
async def filter_city_more_no(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(None)
    await _show_filters(callback)
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("org_filter_direction_page_"))
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


@router.callback_query(lambda c: c.data.startswith("org_filter_direction_pick_"))
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
        await state.update_data(organization_filters_draft=draft)
    await callback.message.answer(
        "Хотите еще выбрать направление?",
        reply_markup=_ask_more_direction_keyboard(),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "org_filter_direction_more_yes")
async def filter_direction_more_yes(callback: CallbackQuery) -> None:
    await _safe_edit_message(
        callback.message, "Выберите направление:", reply_markup=_direction_keyboard(0)
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "org_filter_direction_more_no")
async def filter_direction_more_no(callback: CallbackQuery) -> None:
    await _show_filters(callback)
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("org_filter_type_page_"))
async def filter_type_page(callback: CallbackQuery) -> None:
    try:
        page = int(callback.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректная страница", show_alert=True)
        return
    await _safe_edit_message(
        callback.message, "Выберите тип организации:", reply_markup=_type_keyboard(page)
    )
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("org_filter_type_pick_"))
async def filter_type_pick(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        idx = int(callback.data.rsplit("_", 1)[1])
        org_type = FILTER_TYPES[idx]
    except (ValueError, IndexError):
        await callback.answer("Некорректный тип", show_alert=True)
        return

    _, draft = await _get_filters(state)
    if org_type not in draft["types"]:
        draft["types"].append(org_type)
        await state.update_data(organization_filters_draft=draft)
    await callback.message.answer(
        "Хотите еще выбрать тип организации?",
        reply_markup=_ask_more_type_keyboard(),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "org_filter_type_more_yes")
async def filter_type_more_yes(callback: CallbackQuery) -> None:
    await _safe_edit_message(
        callback.message, "Выберите тип организации:", reply_markup=_type_keyboard(0)
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "org_filter_type_more_no")
async def filter_type_more_no(callback: CallbackQuery) -> None:
    await _show_filters(callback)
    await callback.answer()


@router.callback_query(lambda c: c.data == "org_filters_apply")
async def apply_filters(callback: CallbackQuery, state: FSMContext) -> None:
    _, draft = await _get_filters(state)
    applied = {
        "cities": list(draft.get("cities", [])),
        "directions": list(draft.get("directions", [])),
        "types": list(draft.get("types", [])),
    }
    await state.update_data(organization_filters_applied=applied)
    await _show_organization_by_index(callback, state, index=0)


@router.callback_query(lambda c: c.data == "org_filters_reset")
async def reset_filters(callback: CallbackQuery, state: FSMContext) -> None:
    empty = _empty_filters()
    await state.update_data(
        organization_filters_applied=empty,
        organization_filters_draft=empty,
    )
    await _show_organization_by_index(callback, state, index=0)
