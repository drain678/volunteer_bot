import asyncio
import time

import aio_pika
import msgpack
import logging.config
import re
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from consumer.logger import LOGGING_CONFIG, logger
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from sqlalchemy import select

from config.settings import settings
from src.handlers.callback.router import router
from src.handlers.command.menu import build_menu_by_role
from src.handlers.state.create_organization_profile import OrganizationProfileState
from src.models.models import User
from src.storage.db import async_session
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

ADMIN_TELEGRAM_ID = 1200510460

PENDING_ORGANIZATION_REQUESTS: dict[str, dict] = {}


def _normalize_phone(phone: str) -> str:
    phone = phone.strip()
    has_plus = phone.startswith("+")
    digits = "".join(ch for ch in phone if ch.isdigit())
    return f"+{digits}" if has_plus else digits


def _is_valid_phone(phone: str) -> bool:
    normalized = _normalize_phone(phone)
    if normalized.startswith("+"):
        return bool(re.fullmatch(r"^\+7\d{10}$", normalized))
    return bool(re.fullmatch(r"^8\d{10}$", normalized))


def _direction_keyboard(page: int, show_cancel: bool) -> InlineKeyboardMarkup:
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
            row.append(
                InlineKeyboardButton(
                    text=item, callback_data=f"create_org_direction_pick_{idx}"
                )
            )
        rows.append(row)

    prev_page = (page - 1) % pages
    next_page = (page + 1) % pages
    controls = [
        InlineKeyboardButton(text="⬅️", callback_data=f"create_org_direction_page_{prev_page}")
    ]
    if show_cancel:
        controls.append(
            InlineKeyboardButton(text="Отменить выбор", callback_data="create_org_direction_clear")
        )
    controls.append(
        InlineKeyboardButton(text="➡️", callback_data=f"create_org_direction_page_{next_page}")
    )
    rows.append(controls)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _type_keyboard(page: int) -> InlineKeyboardMarkup:
    page_size = 4
    pages = max(1, (len(FILTER_TYPES) + page_size - 1) // page_size)
    page = page % pages
    start = page * page_size
    chunk = FILTER_TYPES[start:start + page_size]

    rows = [
        [
            InlineKeyboardButton(
                text=item, callback_data=f"create_org_type_pick_{FILTER_TYPES.index(item)}"
            )
        ]
        for item in chunk
    ]

    prev_page = (page - 1) % pages
    next_page = (page + 1) % pages
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️", callback_data=f"create_org_type_page_{prev_page}"
            ),
            InlineKeyboardButton(
                text="➡️", callback_data=f"create_org_type_page_{next_page}"
            ),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _direction_more_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="create_org_direction_more_yes"),
                InlineKeyboardButton(text="Нет", callback_data="create_org_direction_more_no"),
            ]
        ]
    )


async def _safe_edit_message(
    message: Message, text: str, reply_markup: InlineKeyboardMarkup | None = None
) -> None:
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc):
            raise


def _moderation_keyboard(request_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Принять заявку", callback_data=f"org_req_approve_{request_id}"
                ),
                InlineKeyboardButton(
                    text="Отклонить заявку", callback_data=f"org_req_reject_{request_id}"
                ),
            ]
        ]
    )


async def _is_admin(telegram_id: int) -> bool:
    async with async_session() as db:
        result = await db.execute(select(User).where(User.telegram_id == telegram_id))
        user = result.scalar_one_or_none()
        return bool(user and user.role == "admin")


async def _request_create_organization(profile_payload: dict) -> dict | None:
    applicant_id = int(profile_payload["id"])
    async with channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        queue = await channel.declare_queue("user_messages", durable=True)
        user_queue = await channel.declare_queue(
            settings.USER_QUEUE.format(user_id=applicant_id), durable=True
        )
        await queue.bind(exchange, "user_messages")
        await user_queue.bind(
            exchange, settings.USER_QUEUE.format(user_id=applicant_id)
        )

        await exchange.publish(
            aio_pika.Message(msgpack.packb(profile_payload)),
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


@router.callback_query(lambda c: c.data in {"role_organizer"})
async def create_organization(callback: CallbackQuery, state: FSMContext) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("СОЗДАНИЕ ПРОФИЛЯ ОРГАНИЗАЦИИ", extra={"body": callback.from_user.id})
    await state.set_state(OrganizationProfileState.organization_name)
    await state.update_data(role="organizer")
    await callback.message.answer("Название организации?")
    await callback.answer()


@router.message(OrganizationProfileState.organization_name)
async def organization_name(message: Message, state: FSMContext) -> None:
    organization_name = (message.text or "").strip()
    if not organization_name:
        await message.answer("Название организации не должно быть пустым. Введи снова.")
        return

    await state.update_data(organization_name=organization_name)
    await state.set_state(OrganizationProfileState.representative_name)
    await message.answer("Имя представителя организации?")


@router.message(OrganizationProfileState.representative_name)
async def organization_representative_name(message: Message, state: FSMContext) -> None:
    representative_name = (message.text or "").strip()
    if not representative_name:
        await message.answer("Имя представителя не должно быть пустым. Введи снова.")
        return

    await state.update_data(representative_name=representative_name)
    await state.set_state(OrganizationProfileState.representative_phone)
    await message.answer("Номер телефона представителя организации?")


@router.message(OrganizationProfileState.representative_phone)
async def organization_representative_phone(message: Message, state: FSMContext) -> None:
    representative_phone = (message.text or "").strip()
    if not _is_valid_phone(representative_phone):
        await message.answer(
            "Телефон в формате +7 953 698 6160, 8 953 698 6160, "
            "+7 (953) 698-61-60 или 8 (953) 698-61-60."
        )
        return

    await state.update_data(representative_phone=_normalize_phone(representative_phone))
    await state.set_state(OrganizationProfileState.website)
    await message.answer("Сайт организации?")


@router.message(OrganizationProfileState.website)
async def organization_website(message: Message, state: FSMContext) -> None:
    website = (message.text or "").strip()
    if not website:
        await message.answer("Сайт не должен быть пустым. Введи снова.")
        return

    await state.update_data(website=website)
    await state.set_state(OrganizationProfileState.description)
    await message.answer("Краткое описание организации?")


@router.message(OrganizationProfileState.description)
async def organization_description(message: Message, state: FSMContext) -> None:
    description = (message.text or "").strip()
    if not description:
        await message.answer("Описание не должно быть пустым. Введи снова.")
        return

    await state.update_data(description=description)
    await state.set_state(OrganizationProfileState.city)
    await message.answer("Введите название города:")


@router.message(OrganizationProfileState.city)
async def organization_city(message: Message, state: FSMContext) -> None:
    city = (message.text or "").strip()
    if not city:
        await message.answer("Город не должен быть пустым. Введи снова.")
        return
    await state.update_data(city=city, selected_directions=[])
    await state.set_state(OrganizationProfileState.direction_select)
    await message.answer(
        "Выберите направление:",
        reply_markup=_direction_keyboard(0, show_cancel=False),
    )


@router.callback_query(
    StateFilter(OrganizationProfileState.direction_select),
    lambda c: c.data.startswith("create_org_direction_page_"),
)
async def organization_direction_page(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        page = int(callback.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректная страница", show_alert=True)
        return
    data = await state.get_data()
    selected_directions = list(data.get("selected_directions", []))
    await _safe_edit_message(
        callback.message,
        "Выберите направление:",
        reply_markup=_direction_keyboard(page, show_cancel=bool(selected_directions)),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(OrganizationProfileState.direction_select),
    lambda c: c.data.startswith("create_org_direction_pick_"),
)
async def organization_direction_pick(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        idx = int(callback.data.rsplit("_", 1)[1])
        direction = FILTER_DIRECTIONS[idx]
    except (ValueError, IndexError):
        await callback.answer("Некорректное направление", show_alert=True)
        return

    data = await state.get_data()
    selected_directions = list(data.get("selected_directions", []))
    if direction not in selected_directions:
        selected_directions.append(direction)
    await state.update_data(selected_directions=selected_directions)
    await state.set_state(OrganizationProfileState.direction_more)
    await _safe_edit_message(
        callback.message,
        "Хотите еще выбрать направление?",
        reply_markup=_direction_more_keyboard(),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(OrganizationProfileState.direction_more),
    lambda c: c.data == "create_org_direction_more_yes",
)
async def organization_direction_more_yes(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(OrganizationProfileState.direction_select)
    await _safe_edit_message(
        callback.message,
        "Выберите направление:",
        reply_markup=_direction_keyboard(0, show_cancel=True),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(OrganizationProfileState.direction_more),
    lambda c: c.data == "create_org_direction_more_no",
)
async def organization_direction_more_no(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(OrganizationProfileState.type_select)
    await _safe_edit_message(
        callback.message,
        "Выберите тип организации:",
        reply_markup=_type_keyboard(0),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(OrganizationProfileState.type_select),
    lambda c: c.data.startswith("create_org_type_page_"),
)
async def organization_type_page(callback: CallbackQuery) -> None:
    try:
        page = int(callback.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректная страница", show_alert=True)
        return
    await _safe_edit_message(
        callback.message, "Выберите тип организации:", reply_markup=_type_keyboard(page)
    )
    await callback.answer()


@router.callback_query(
    StateFilter(OrganizationProfileState.direction_select),
    lambda c: c.data == "create_org_direction_clear",
)
async def organization_direction_clear(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    selected_directions = list(data.get("selected_directions", []))
    if not selected_directions:
        await callback.answer("Сначала выберите хотя бы одно направление", show_alert=True)
        return
    await state.set_state(OrganizationProfileState.type_select)
    await _safe_edit_message(
        callback.message,
        "Выберите тип организации:",
        reply_markup=_type_keyboard(0),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(OrganizationProfileState.type_select),
    lambda c: c.data.startswith("create_org_type_pick_"),
)
async def organization_type_pick(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        idx = int(callback.data.rsplit("_", 1)[1])
        org_type = FILTER_TYPES[idx]
    except (ValueError, IndexError):
        await callback.answer("Некорректный тип", show_alert=True)
        return

    await state.update_data(type_organization=org_type)
    profile_data = await state.get_data()
    direction = ", ".join(profile_data.get("selected_directions", []))
    body = {
        "action": "make_organization_form",
        "id": callback.from_user.id,
        "role": "organizer",
        "organization_name": profile_data.get("organization_name"),
        "representative_name": profile_data.get("representative_name"),
        "representative_phone": profile_data.get("representative_phone"),
        "website": profile_data.get("website"),
        "description": profile_data.get("description"),
        "city": profile_data.get("city"),
        "direction": direction,
        "type_organization": profile_data.get("type_organization"),
    }

    request_id = f"{callback.from_user.id}_{int(time.time())}"
    PENDING_ORGANIZATION_REQUESTS[request_id] = body
    try:
        await callback.bot.send_message(
            ADMIN_TELEGRAM_ID, "Новая заявка на профиль организатора"
        )
        await callback.bot.send_message(
            ADMIN_TELEGRAM_ID,
            render("profile_organization.jinja2", user=body),
            reply_markup=_moderation_keyboard(request_id),
        )
    except TelegramBadRequest:
        pass

    await callback.message.answer(
        "Заявка отправлена администратору на проверку. Ожидайте решение."
    )
    await state.clear()
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("org_req_approve_"))
async def approve_org_request(callback: CallbackQuery) -> None:
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Только администратор может подтверждать заявку", show_alert=True)
        return
    request_id = callback.data.removeprefix("org_req_approve_")
    payload = PENDING_ORGANIZATION_REQUESTS.get(request_id)
    if not payload:
        await callback.answer("Заявка не найдена или уже обработана", show_alert=True)
        return

    result = await _request_create_organization(payload)
    if not result or "error" in result:
        await callback.answer("Не удалось создать профиль организатора", show_alert=True)
        return

    PENDING_ORGANIZATION_REQUESTS.pop(request_id, None)
    applicant_id = payload.get("id")
    try:
        await callback.bot.send_message(
            applicant_id, "Ваша заявка одобрена. Профиль организатора создан."
        )
        await callback.bot.send_message(
            applicant_id, render("profile_organization.jinja2", user=result)
        )
        await callback.bot.send_message(
            applicant_id, "Меню бота:", reply_markup=build_menu_by_role("organizer")
        )
    except TelegramBadRequest:
        pass

    await callback.answer("Заявка подтверждена", show_alert=True)


@router.callback_query(lambda c: c.data.startswith("org_req_reject_"))
async def reject_org_request_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Только администратор может отклонять заявку", show_alert=True)
        return
    request_id = callback.data.removeprefix("org_req_reject_")
    if request_id not in PENDING_ORGANIZATION_REQUESTS:
        await callback.answer("Заявка не найдена или уже обработана", show_alert=True)
        return
    await state.set_state(OrganizationProfileState.moderation_reject_reason)
    await state.update_data(reject_org_request_id=request_id)
    await callback.message.answer("Напишите причину отклонения:")
    await callback.answer()


@router.message(OrganizationProfileState.moderation_reject_reason)
async def reject_org_request_reason(message: Message, state: FSMContext) -> None:
    if not await _is_admin(message.from_user.id):
        await message.answer("Только администратор может отклонять заявку.")
        await state.clear()
        return

    reason = (message.text or "").strip()
    if not reason:
        await message.answer("Причина не должна быть пустой.")
        return

    data = await state.get_data()
    request_id = data.get("reject_org_request_id")
    payload = PENDING_ORGANIZATION_REQUESTS.pop(request_id, None)
    if not payload:
        await message.answer("Заявка не найдена или уже обработана.")
        await state.clear()
        return

    applicant_id = payload.get("id")
    try:
        await message.bot.send_message(
            applicant_id,
            "Благодарим за проявленный интерес, но ваша заявка была отклонена по причине: "
            f"{reason}",
        )
    except TelegramBadRequest:
        pass
    await message.answer("Заявка отклонена.")
    await state.clear()
