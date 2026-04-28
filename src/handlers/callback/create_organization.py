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
from aiogram.types import CallbackQuery, Message

from config.settings import settings
from src.handlers.callback.router import router
from src.handlers.command.menu import build_menu_by_role
from src.handlers.state.create_organization_profile import OrganizationProfileState
from src.storage.rabbit import channel_pool


def _organization_profile_text(profile: dict) -> str:
    website = (profile.get("website") or "").strip()
    site_line = "🌐 <u>Сайт организации</u>: не указан"
    if website:
        site_line = f'🌐 <a href="{website}"><u>Сайт организации</u></a>'

    return (
        "<b>Профиль организации</b>\n\n"
        f"🏢 Название: {profile.get('organization_name')}\n"
        f"👤 Представитель: {profile.get('representative_name')}\n"
        f"📞 Телефон: {profile.get('representative_phone')}\n"
        f"{site_line}\n"
        f"📝 Описание: {profile.get('description')}\n"
    )


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
    if not re.fullmatch(r"^\+?\d{11}$", representative_phone):
        await message.answer("Телефон должен быть в формате 11 цифр или + и 11 цифр.")
        return

    await state.update_data(representative_phone=representative_phone)
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

    profile_data = await state.get_data()
    body = {
        "action": "make_organization_form",
        "id": message.from_user.id,
        "role": "organizer",
        "organization_name": profile_data.get("organization_name"),
        "representative_name": profile_data.get("representative_name"),
        "representative_phone": profile_data.get("representative_phone"),
        "website": profile_data.get("website"),
        "description": description,
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
        await user_queue.bind(
            exchange, settings.USER_QUEUE.format(user_id=message.from_user.id)
        )

        await exchange.publish(
            aio_pika.Message(msgpack.packb(body)),
            routing_key="user_messages",
        )
        logger.info("ОТПРАВИЛИ ЗАПРОС НА СОЗДАНИЕ ПРОФИЛЯ ОРГАНИЗАЦИИ В БД", extra={"body": message.from_user.id})

        for _ in range(10):
            try:
                res = await user_queue.get(timeout=3)
                await res.ack()
                result = msgpack.unpackb(res.body)
                if "error" in result:
                    await message.answer("Не удалось создать профиль. Попробуй позже.")
                    return

                await message.answer("Профиль успешно создан!")
                await message.answer(_organization_profile_text(result))
                await message.answer(
                    "Меню бота:", reply_markup=build_menu_by_role("organizer")
                )
                await state.clear()
                return
            except QueueEmpty:
                logger.info("ОТВЕТ ОТ БД НЕ ПОЛУЧЕН, ОЧЕРЕДЬ ПУСТА", extra={"body": message.from_user.id})
                await asyncio.sleep(1)

    await message.answer("Не удалось создать профиль. Попробуй позже.")
