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
    if age < 10 or age > 100:
        await message.answer("Укажи возраст в диапазоне от 10 до 100.")
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
    if not re.fullmatch(r"^\+?\d{11}$", phone):
        await message.answer("Телефон должен быть в формате 11 цифр или + и 11 цифр.")
        return

    await state.update_data(phone=phone)
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
    profile_data = await state.get_data()

    body = {
        "action": "make_form",
        "id": callback.from_user.id,
        "role": profile_data.get("role", "volunteer"),
        "name": profile_data.get("name"),
        "age": profile_data.get("age"),
        "city": profile_data.get("city"),
        "phone": profile_data.get("phone"),
        "gender": gender,
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