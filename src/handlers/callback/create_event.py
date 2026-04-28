import asyncio
from datetime import datetime
import logging
import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from config.settings import settings
from src.handlers.callback.router import router
from src.handlers.state.create_event import CreateEventState
from src.storage.rabbit import channel_pool
from consumer.logger import LOGGING_CONFIG, logger

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
    if min_age < 0 or min_age > 100:
        await message.answer("Укажи возраст в диапазоне 0-100.")
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
        datetime.strptime(start_time, "%d.%m.%Y %H:%M")
    except ValueError:
        await message.answer("Неверный формат. Пример: 30.04.2026 14:30")
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
    await message.answer("Категория мероприятия?")


@router.message(CreateEventState.category)
async def event_category(message: Message, state: FSMContext) -> None:
    category = (message.text or "").strip()
    if not category:
        await message.answer("Категория не должна быть пустой. Введи снова.")
        return

    event_data = await state.get_data()
    body = {
        "action": "create_event",
        "id": message.from_user.id,
        "title": event_data.get("title"),
        "description": event_data.get("description"),
        "min_age": event_data.get("min_age"),
        "city": event_data.get("city"),
        "start_time": event_data.get("start_time"),
        "duration_hours": event_data.get("duration_hours"),
        "category": category,
    }

    async with channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        queue = await channel.declare_queue("user_messages", durable=True)
        user_queue = await channel.declare_queue(
            settings.USER_QUEUE.format(user_id=message.from_user.id), durable=True
        )
        await queue.bind(exchange, "user_messages")
        await user_queue.bind(exchange, settings.USER_QUEUE.format(user_id=message.from_user.id))

        await exchange.publish(
            aio_pika.Message(msgpack.packb(body)),
            routing_key="user_messages",
        )
        logger.info("ОТПРАВИЛИ ЗАПРОС НА СОЗДАНИЕ МЕРОПРИЯТИЯ В БД", extra={"body": message.from_user.id})

        for _ in range(10):
            try:
                res = await user_queue.get(timeout=3)
                await res.ack()
                result = msgpack.unpackb(res.body)
                if "error" in result:
                    await message.answer("Не удалось создать мероприятие. Попробуй позже.")
                    return

                await message.answer("Мероприятие успешно создано!")
                await message.answer(
                    f"<b>{result.get('title')}</b>\n"
                    f"📝 {result.get('description')}\n"
                    f"🎯 Мин. возраст: {result.get('min_age')}\n"
                    f"📍 Город: {result.get('city')}\n"
                    f"🕒 Старт: {result.get('start_time')}\n"
                    f"⏳ Длительность: {result.get('duration_hours')} ч.\n"
                    f"🏷 Категория: {result.get('category')}"
                )
                await state.clear()
                return
            except QueueEmpty:
                logger.info("ОЖИДАЕМ ОТВЕТ НА СОЗДАНИЕ МЕРОПРИЯТИЯ В БД", extra={"body": message.from_user.id})
                await asyncio.sleep(1)

    await message.answer("Не удалось создать мероприятие. Попробуй позже.")
