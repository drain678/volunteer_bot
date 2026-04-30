import asyncio

import aio_pika
import msgpack
import logging.config
import re
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from consumer.logger import LOGGING_CONFIG, logger
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config.settings import settings
from src.handlers.callback.router import router
from src.handlers.command.menu import build_menu_by_role
from src.handlers.state.create_profile import VolunteerProfileState
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


def _yes_no_keyboard(yes_data: str, no_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data=yes_data),
                InlineKeyboardButton(text="Нет", callback_data=no_data),
            ]
        ]
    )


def _directions_keyboard(page: int) -> InlineKeyboardMarkup:
    page_size = 4
    pages = max(1, (len(FILTER_DIRECTIONS) + page_size - 1) // page_size)
    page = page % pages
    start = page * page_size
    chunk = FILTER_DIRECTIONS[start:start + page_size]

    rows = [
        [InlineKeyboardButton(text="Выбрать все", callback_data="pref_direction_pick_all")]
    ]
    for i in range(0, len(chunk), 2):
        row = []
        for item in chunk[i:i + 2]:
            idx = FILTER_DIRECTIONS.index(item)
            row.append(InlineKeyboardButton(text=item, callback_data=f"pref_direction_pick_{idx}"))
        rows.append(row)

    prev_page = (page - 1) % pages
    next_page = (page + 1) % pages
    rows.append(
        [
            InlineKeyboardButton(text="⬅️", callback_data=f"pref_direction_page_{prev_page}"),
            InlineKeyboardButton(text="➡️", callback_data=f"pref_direction_page_{next_page}"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(lambda c: c.data in {"role_volunteer"})
async def create_profile(callback: CallbackQuery, state: FSMContext) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("СОЗДАНИЕ ПРОФИЛЯ ВОЛОНТЕРА", extra={"body": callback.from_user.id})

    await state.set_state(VolunteerProfileState.name)
    await state.update_data(role="volunteer")
    await callback.message.answer("Как тебя зовут?")
    await callback.answer()


@router.message(VolunteerProfileState.name)
async def volunteer_name(message: Message, state: FSMContext) -> None:
    name = (message.text or "").strip()
    if not name:
        await message.answer("Имя не должно быть пустым. Введи, пожалуйста, имя.")
        return

    await state.update_data(name=name)
    await state.set_state(VolunteerProfileState.age)
    await message.answer("Сколько тебе лет?")


@router.message(VolunteerProfileState.age)
async def volunteer_age(message: Message, state: FSMContext) -> None:
    age_text = (message.text or "").strip()
    if not age_text.isdigit():
        await message.answer("Возраст должен быть числом. Попробуй еще раз.")
        return

    age = int(age_text)
    if age < 14 or age > 100:
        await message.answer("Укажи возраст в диапазоне от 14 до 100.")
        return

    await state.update_data(age=age)
    await state.set_state(VolunteerProfileState.city)
    await message.answer("Город проживания?")


@router.message(VolunteerProfileState.city)
async def volunteer_city(message: Message, state: FSMContext) -> None:
    city = (message.text or "").strip()
    if not city:
        await message.answer("Город не должен быть пустым. Введи, пожалуйста, город.")
        return

    await state.update_data(city=city)
    await state.set_state(VolunteerProfileState.phone)
    await message.answer("Номер телефона?")


@router.message(VolunteerProfileState.phone)
async def volunteer_phone(message: Message, state: FSMContext) -> None:
    phone = (message.text or "").strip()
    if not _is_valid_phone(phone):
        await message.answer(
            "Телефон в формате +7 953 698 6160, 8 953 698 6160, "
            "+7 (953) 698-61-60 или 8 (953) 698-61-60."
        )
        return

    await state.update_data(phone=_normalize_phone(phone))
    await state.set_state(VolunteerProfileState.gender)

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Ж", callback_data="gender_f")],
            [InlineKeyboardButton(text="М", callback_data="gender_m")],
        ]
    )
    await message.answer("Пол:", reply_markup=keyboard)


@router.callback_query(
    StateFilter(VolunteerProfileState.gender),
    lambda c: c.data in {"gender_f", "gender_m"},
)
async def volunteer_gender(callback: CallbackQuery, state: FSMContext) -> None:
    gender = "f" if callback.data == "gender_f" else "m"
    await state.update_data(gender=gender, preferred_cities=[])
    await state.set_state(VolunteerProfileState.preferred_city_input)
    await callback.message.answer(
        'Для какого города вы хотите видеть мероприятия? Если для всех, напишите "все".'
    )
    await callback.answer()


@router.message(VolunteerProfileState.preferred_city_input)
async def volunteer_preferred_city_input(message: Message, state: FSMContext) -> None:
    city = (message.text or "").strip()
    if not city:
        await message.answer("Город не должен быть пустым. Введи снова.")
        return
    data = await state.get_data()
    cities = list(data.get("preferred_cities", []))
    lowered = {item.lower() for item in cities}
    if city.lower() == "все":
        await state.update_data(all_cities=True, preferred_cities=[])
        await state.set_state(VolunteerProfileState.preferred_direction_select)
        await message.answer(
            "Выберите интересующие направления мероприятий:",
            reply_markup=_directions_keyboard(0),
        )
        return
    if city.lower() not in lowered:
        cities.append(city)
    await state.update_data(all_cities=False, preferred_cities=cities)
    await state.set_state(VolunteerProfileState.preferred_city_more)
    await message.answer(
        "Хотите еще добавить город?",
        reply_markup=_yes_no_keyboard("pref_city_more_yes", "pref_city_more_no"),
    )


@router.callback_query(
    StateFilter(VolunteerProfileState.preferred_city_more),
    lambda c: c.data == "pref_city_more_yes",
)
async def volunteer_pref_city_more_yes(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(VolunteerProfileState.preferred_city_input)
    await callback.message.answer(
        'Введите еще город. Если хотите все города, напишите "все".'
    )
    await callback.answer()


@router.callback_query(
    StateFilter(VolunteerProfileState.preferred_city_more),
    lambda c: c.data == "pref_city_more_no",
)
async def volunteer_pref_city_more_no(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(VolunteerProfileState.preferred_direction_select)
    await callback.message.answer(
        "Выберите интересующие направления мероприятий:",
        reply_markup=_directions_keyboard(0),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(VolunteerProfileState.preferred_direction_select),
    lambda c: c.data.startswith("pref_direction_page_"),
)
async def volunteer_pref_direction_page(callback: CallbackQuery) -> None:
    try:
        page = int(callback.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректная страница", show_alert=True)
        return
    await callback.message.edit_reply_markup(reply_markup=_directions_keyboard(page))
    await callback.answer()


@router.callback_query(
    StateFilter(VolunteerProfileState.preferred_direction_select),
    lambda c: c.data == "pref_direction_pick_all",
)
async def volunteer_pref_direction_all(callback: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(all_directions=True, preferred_directions=[])
    await state.set_state(VolunteerProfileState.preferred_direction_more)
    await callback.message.answer(
        "Вы выбрали все направления мероприятий. Хотите изменить выбор?",
        reply_markup=_yes_no_keyboard("pref_direction_more_yes", "pref_direction_more_no"),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(VolunteerProfileState.preferred_direction_select),
    lambda c: c.data.startswith("pref_direction_pick_"),
)
async def volunteer_pref_direction_pick(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        idx = int(callback.data.rsplit("_", 1)[1])
        direction = FILTER_DIRECTIONS[idx]
    except (ValueError, IndexError):
        await callback.answer("Некорректное направление", show_alert=True)
        return
    data = await state.get_data()
    selected = list(data.get("preferred_directions", []))
    if direction not in selected:
        selected.append(direction)
    await state.update_data(all_directions=False, preferred_directions=selected)
    await state.set_state(VolunteerProfileState.preferred_direction_more)
    await callback.message.answer(
        "Хотите еще выбрать направление?",
        reply_markup=_yes_no_keyboard("pref_direction_more_yes", "pref_direction_more_no"),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(VolunteerProfileState.preferred_direction_more),
    lambda c: c.data == "pref_direction_more_yes",
)
async def volunteer_pref_direction_more_yes(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(VolunteerProfileState.preferred_direction_select)
    await callback.message.answer(
        "Выберите интересующие  мероприятий:",
        reply_markup=_directions_keyboard(0),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(VolunteerProfileState.preferred_direction_more),
    lambda c: c.data == "pref_direction_more_no",
)
async def volunteer_pref_direction_more_no(callback: CallbackQuery, state: FSMContext) -> None:
    profile_data = await state.get_data()
    preferred_cities = list(profile_data.get("preferred_cities", []))
    preferred_directions = list(profile_data.get("preferred_directions", []))
    if not profile_data.get("all_directions") and not preferred_directions:
        await callback.answer("Выберите хотя бы одно направление или 'Выбрать все'", show_alert=True)
        return

    body = {
        "action": "make_form",
        "id": callback.from_user.id,
        "role": profile_data.get("role", "volunteer"),
        "name": profile_data.get("name"),
        "age": profile_data.get("age"),
        "city": profile_data.get("city"),
        "phone": profile_data.get("phone"),
        "gender": profile_data.get("gender"),
        "all_cities": bool(profile_data.get("all_cities")),
        "all_directions": bool(profile_data.get("all_directions")),
        "preferred_cities": ", ".join(preferred_cities),
        "preferred_directions": ", ".join(preferred_directions),
    }

    async with channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        queue = await channel.declare_queue("user_messages", durable=True)
        user_queue = await channel.declare_queue(
            settings.USER_QUEUE.format(user_id=callback.from_user.id),
            durable=True,
        )

        await queue.bind(exchange, "user_messages")
        await user_queue.bind(
            exchange, settings.USER_QUEUE.format(user_id=callback.from_user.id)
        )

        await exchange.publish(
            aio_pika.Message(msgpack.packb(body)),
            routing_key="user_messages",
        )
        logger.info("ОТПРАВИЛИ ЗАПРОС НА СОЗДАНИЕ ПРОФИЛЯ ВОЛОНТЕРА В БД", extra={"body": callback.from_user.id})

        for _ in range(10):
            try:
                res = await user_queue.get(timeout=3)
                await res.ack()
                result = msgpack.unpackb(res.body)

                if "error" in result:
                    await callback.message.answer("Не удалось создать профиль")
                    await callback.answer()
                    return

                await callback.message.answer("Профиль успешно создан!")
                await callback.message.answer(render("profile.jinja2", user=result))
                await callback.message.answer(
                    "Меню бота:", reply_markup=build_menu_by_role("volunteer")
                )
                await state.clear()
                await callback.answer()
                return
            except QueueEmpty:
                logger.info("ОТВЕТ ОТ БД НЕ ПОЛУЧЕН, ОЧЕРЕДЬ ПУСТА", extra={"body": callback.from_user.id})
                await asyncio.sleep(1)

    await callback.message.answer("Не удалось создать профиль")
    await callback.answer()