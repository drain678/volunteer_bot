import asyncio
import logging
import aio_pika
import msgpack
import re
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.fsm.context import FSMContext
from consumer.logger import LOGGING_CONFIG, logger
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.types import CallbackQuery, Message

from config.settings import settings
from src.handlers.callback.get_profile import get_profile as show_profile
from src.handlers.callback.router import router
from src.handlers.state.create_profile import VolunteerProfileState
from src.handlers.state.edit_profile import EditProfileState
from src.storage.rabbit import channel_pool

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


def edit_fields_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(text="Имя", callback_data="edit_field_name"),
            InlineKeyboardButton(text="Возраст", callback_data="edit_field_age"),
        ],
        [
            InlineKeyboardButton(text="Город", callback_data="edit_field_city"),
            InlineKeyboardButton(text="Телефон", callback_data="edit_field_phone"),
        ],
        [
            InlineKeyboardButton(text="Города мероприятий", callback_data="edit_field_preferred_cities"),
            InlineKeyboardButton(text="Направления мероприятий", callback_data="edit_field_preferred_directions"),
        ],
    ]
    buttons.append(
        [InlineKeyboardButton(text="Создать профиль заново", callback_data="recreate_profile")]
    )
    buttons.append([InlineKeyboardButton(text="Назад", callback_data="back_to_profile")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def edit_more_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="edit_more_yes"),
                InlineKeyboardButton(text="Нет", callback_data="edit_more_no"),
            ]
        ]
    )


def _yes_no_keyboard(yes_data: str, no_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data=yes_data),
                InlineKeyboardButton(text="Нет", callback_data=no_data),
            ]
        ]
    )


def _directions_keyboard(page: int, show_cancel: bool) -> InlineKeyboardMarkup:
    page_size = 4
    pages = max(1, (len(FILTER_DIRECTIONS) + page_size - 1) // page_size)
    page = page % pages
    start = page * page_size
    chunk = FILTER_DIRECTIONS[start:start + page_size]

    rows = [[InlineKeyboardButton(text="Выбрать все", callback_data="edit_pref_direction_all")]]
    for i in range(0, len(chunk), 2):
        row = []
        for item in chunk[i:i + 2]:
            idx = FILTER_DIRECTIONS.index(item)
            row.append(InlineKeyboardButton(text=item, callback_data=f"edit_pref_direction_pick_{idx}"))
        rows.append(row)

    prev_page = (page - 1) % pages
    next_page = (page + 1) % pages
    controls = [InlineKeyboardButton(text="⬅️", callback_data=f"edit_pref_direction_page_{prev_page}")]
    if show_cancel:
        controls.append(InlineKeyboardButton(text="Отменить выбор", callback_data="edit_pref_direction_cancel"))
    controls.append(InlineKeyboardButton(text="➡️", callback_data=f"edit_pref_direction_page_{next_page}"))
    rows.append(controls)
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def request_to_consumer(payload: dict) -> dict | None:
    user_id = payload["id"]
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

        await exchange.publish(
            aio_pika.Message(msgpack.packb(payload)),
            "user_messages",
        )
        logger.info("ОТПРАВИЛИ ЗАПРОС НА ОБНОВЛЕНИЕ ПРОФИЛЯ В БД", extra={"body": user_id})

        for _ in range(10):
            try:
                res = await user_queue.get()
                await res.ack()
                return msgpack.unpackb(res.body)
            except QueueEmpty:
                logger.info("ОТВЕТ ОТ БД НЕ ПОЛУЧЕН, ОЧЕРЕДЬ ПУСТА", extra={"body": user_id})
                await asyncio.sleep(1)
    return None


@router.callback_query(lambda c: c.data == "edit_profile")
async def start_edit_profile(callback: CallbackQuery, state: FSMContext) -> None:
    profile = await request_to_consumer({"id": callback.from_user.id, "action": "get_profile"})
    if not profile or "error" in profile:
        await callback.answer("Профиль не найден", show_alert=True)
        return
    logger.info("НАЧАЛО ИЗМЕНЕНИЯ ПРОФИЛЯ", extra={"body": callback.from_user.id})
    await callback.message.answer(
        "Что вы хотите изменить?",
        reply_markup=edit_fields_keyboard(),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "recreate_profile")
async def recreate_profile(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(VolunteerProfileState.name)
    await callback.message.answer("Как тебя зовут?")
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("edit_field_"))
async def choose_edit_field(callback: CallbackQuery, state: FSMContext) -> None:
    field = callback.data.replace("edit_field_", "", 1)
    if field == "preferred_cities":
        await state.set_state(EditProfileState.preferred_city_input)
        await state.update_data(edit_pref_cities=[])
        await callback.message.answer(
            'Для какого города вы хотите видеть мероприятия? Если для всех, то напишите "все".'
        )
        await callback.answer()
        return
    if field == "preferred_directions":
        await state.set_state(EditProfileState.preferred_direction_select)
        await state.update_data(edit_pref_directions=[])
        await callback.message.answer(
            "Выберите интересующие направления мероприятий:",
            reply_markup=_directions_keyboard(page=0, show_cancel=False),
        )
        await callback.answer()
        return
    prompts = {
        "name": "Введи новое имя:",
        "age": "Введи новый возраст:",
        "city": "Введи новый город:",
        "phone": "Введи новый телефон:",
    }
    await state.set_state(EditProfileState.waiting_new_value)
    await state.update_data(edit_field=field)
    await callback.message.answer(prompts.get(field, "Введи новое значение:"))
    await callback.answer()


@router.message(EditProfileState.waiting_new_value)
async def update_profile_value(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    field = data.get("edit_field")
    value = (message.text or "").strip()
    if not field or not value:
        await message.answer("Введите корректное значение.")
        return

    if field == "age" and not value.isdigit():
        await message.answer("Возраст должен быть числом.")
        return
    if field == "age" and int(value) < 14:
        await message.answer("Возраст волонтера не может быть меньше 14 лет.")
        return

    if field == "phone" and not _is_valid_phone(value):
        await message.answer(
            "Телефон в формате +7 953 698 6160, 8 953 698 6160, "
            "+7 (953) 698-61-60 или 8 (953) 698-61-60."
        )
        return
    if field == "phone":
        value = _normalize_phone(value)
    result = await request_to_consumer(
        {
            "id": message.from_user.id,
            "action": "update_profile",
            "field": field,
            "value": value,
        }
    )
    if not result or "error" in result:
        await message.answer("Не удалось обновить профиль. Попробуй позже.")
        return

    await state.clear()
    await message.answer("Хочешь изменить что-то еще?", reply_markup=edit_more_keyboard())


@router.message(EditProfileState.preferred_city_input)
async def edit_pref_city_input(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Город не должен быть пустым.")
        return
    if text.lower() == "все":
        result = await request_to_consumer(
            {"id": message.from_user.id, "action": "update_profile", "field": "all_cities", "value": True}
        )
        if not result or "error" in result:
            await message.answer("Не удалось обновить профиль. Попробуй позже.")
            return
        result = await request_to_consumer(
            {"id": message.from_user.id, "action": "update_profile", "field": "preferred_cities", "value": ""}
        )
        if not result or "error" in result:
            await message.answer("Не удалось обновить профиль. Попробуй позже.")
            return
        await state.clear()
        await message.answer("Хочешь изменить что-то еще?", reply_markup=edit_more_keyboard())
        return

    data = await state.get_data()
    cities = list(data.get("edit_pref_cities", []))
    if text.lower() not in {item.lower() for item in cities}:
        cities.append(text)
    await state.update_data(edit_pref_cities=cities)
    await state.set_state(EditProfileState.preferred_city_more)
    await message.answer(
        "Хотите еще добавить город?",
        reply_markup=_yes_no_keyboard("edit_pref_city_more_yes", "edit_pref_city_more_no"),
    )


@router.callback_query(
    lambda c: c.data in {"edit_pref_city_more_yes", "edit_pref_city_more_no"}
)
async def edit_pref_city_more(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.data == "edit_pref_city_more_yes":
        await state.set_state(EditProfileState.preferred_city_input)
        await callback.message.answer("Введите еще город или напишите 'все'.")
        await callback.answer()
        return

    data = await state.get_data()
    cities = list(data.get("edit_pref_cities", []))
    if not cities:
        await callback.answer("Выберите хотя бы один город или 'все'.", show_alert=True)
        return
    result = await request_to_consumer(
        {"id": callback.from_user.id, "action": "update_profile", "field": "all_cities", "value": False}
    )
    if not result or "error" in result:
        await callback.message.answer("Не удалось обновить профиль. Попробуй позже.")
        await callback.answer()
        return
    result = await request_to_consumer(
        {
            "id": callback.from_user.id,
            "action": "update_profile",
            "field": "preferred_cities",
            "value": ", ".join(cities),
        }
    )
    if not result or "error" in result:
        await callback.message.answer("Не удалось обновить профиль. Попробуй позже.")
        await callback.answer()
        return
    await state.clear()
    await callback.message.answer("Хочешь изменить что-то еще?", reply_markup=edit_more_keyboard())
    await callback.answer()


@router.callback_query(
    lambda c: c.data.startswith("edit_pref_direction_page_")
)
async def edit_pref_direction_page(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        page = int(callback.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректная страница", show_alert=True)
        return
    data = await state.get_data()
    selected = list(data.get("edit_pref_directions", []))
    await callback.message.edit_reply_markup(
        reply_markup=_directions_keyboard(page=page, show_cancel=bool(selected))
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "edit_pref_direction_all")
async def edit_pref_direction_all(callback: CallbackQuery, state: FSMContext) -> None:
    result = await request_to_consumer(
        {"id": callback.from_user.id, "action": "update_profile", "field": "all_directions", "value": True}
    )
    if not result or "error" in result:
        await callback.message.answer("Не удалось обновить профиль. Попробуй позже.")
        await callback.answer()
        return
    result = await request_to_consumer(
        {"id": callback.from_user.id, "action": "update_profile", "field": "preferred_directions", "value": ""}
    )
    if not result or "error" in result:
        await callback.message.answer("Не удалось обновить профиль. Попробуй позже.")
        await callback.answer()
        return
    await state.clear()
    await callback.message.answer("Хочешь изменить что-то еще?", reply_markup=edit_more_keyboard())
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("edit_pref_direction_pick_"))
async def edit_pref_direction_pick(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        idx = int(callback.data.rsplit("_", 1)[1])
        direction = FILTER_DIRECTIONS[idx]
    except (ValueError, IndexError):
        await callback.answer("Некорректное направление", show_alert=True)
        return
    data = await state.get_data()
    selected = list(data.get("edit_pref_directions", []))
    if direction not in selected:
        selected.append(direction)
    await state.update_data(edit_pref_directions=selected)
    await state.set_state(EditProfileState.preferred_direction_more)
    await callback.message.answer(
        "Хотите еще выбрать направление?",
        reply_markup=_yes_no_keyboard("edit_pref_direction_more_yes", "edit_pref_direction_more_no"),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "edit_pref_direction_cancel")
async def edit_pref_direction_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    selected = list(data.get("edit_pref_directions", []))
    if not selected:
        await callback.answer("Сначала выберите хотя бы одно направление", show_alert=True)
        return
    await state.set_state(EditProfileState.preferred_direction_more)
    await callback.message.answer(
        "Хотите еще выбрать направление?",
        reply_markup=_yes_no_keyboard("edit_pref_direction_more_yes", "edit_pref_direction_more_no"),
    )
    await callback.answer()


@router.callback_query(
    lambda c: c.data in {"edit_pref_direction_more_yes", "edit_pref_direction_more_no"}
)
async def edit_pref_direction_more(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.data == "edit_pref_direction_more_yes":
        await state.set_state(EditProfileState.preferred_direction_select)
        data = await state.get_data()
        selected = list(data.get("edit_pref_directions", []))
        await callback.message.answer(
            "Выберите интересующие направления мероприятий:",
            reply_markup=_directions_keyboard(page=0, show_cancel=bool(selected)),
        )
        await callback.answer()
        return

    data = await state.get_data()
    selected = list(data.get("edit_pref_directions", []))
    if not selected:
        await callback.answer("Выберите хотя бы одно направление или 'Выбрать все'", show_alert=True)
        return
    result = await request_to_consumer(
        {"id": callback.from_user.id, "action": "update_profile", "field": "all_directions", "value": False}
    )
    if not result or "error" in result:
        await callback.message.answer("Не удалось обновить профиль. Попробуй позже.")
        await callback.answer()
        return
    result = await request_to_consumer(
        {
            "id": callback.from_user.id,
            "action": "update_profile",
            "field": "preferred_directions",
            "value": ", ".join(selected),
        }
    )
    if not result or "error" in result:
        await callback.message.answer("Не удалось обновить профиль. Попробуй позже.")
        await callback.answer()
        return
    await state.clear()
    await callback.message.answer("Хочешь изменить что-то еще?", reply_markup=edit_more_keyboard())
    await callback.answer()


@router.callback_query(lambda c: c.data == "edit_more_yes")
async def edit_more_yes(callback: CallbackQuery) -> None:
    await callback.message.answer(
        "Что вы хотите изменить?",
        reply_markup=edit_fields_keyboard(),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "edit_more_no")
async def edit_more_no(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.message.answer("Профиль обновлен!")
    await show_profile(callback, state)
