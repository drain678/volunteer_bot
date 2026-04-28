import asyncio
import re

import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config.settings import settings
from src.handlers.callback.get_organization import get_organization as show_organization
from src.handlers.callback.router import router
from src.handlers.state.create_organization_profile import OrganizationProfileState
from src.handlers.state.edit_organization import EditOrganizationState
from src.storage.rabbit import channel_pool


def edit_org_fields_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Название", callback_data="edit_org_field_organization_name"),
                InlineKeyboardButton(text="Имя представителя", callback_data="edit_org_field_representative_name"),
            ],
            [
                InlineKeyboardButton(text="Телефон", callback_data="edit_org_field_representative_phone"),
                InlineKeyboardButton(text="Сайт", callback_data="edit_org_field_website"),
            ],
            [InlineKeyboardButton(text="Описание", callback_data="edit_org_field_description")],
            [InlineKeyboardButton(text="Создать профиль заново", callback_data="recreate_organization_profile")],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_organization")],
        ]
    )


def edit_org_more_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="edit_org_more_yes"),
                InlineKeyboardButton(text="Нет", callback_data="edit_org_more_no"),
            ]
        ]
    )


async def request_to_consumer(payload: dict) -> dict | None:
    user_id = payload["id"]
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
        await exchange.publish(aio_pika.Message(msgpack.packb(payload)), "user_messages")

        logger.info("ОТПРАВИЛИ ЗАПРОС НА ОБНОВЛЕНИЕ ПРОФИЛЯ ОРГАНИЗАЦИИ В БД", extra={"body": user_id})

        for _ in range(10):
            try:
                res = await user_queue.get()
                await res.ack()
                return msgpack.unpackb(res.body)
            except QueueEmpty:
                await asyncio.sleep(1)
    return None


@router.callback_query(lambda c: c.data == "edit_organization")
async def start_edit_organization(callback: CallbackQuery, state: FSMContext) -> None:
    logger.info("НАЧАЛО ИЗМЕНЕНИЯ ПРОФИЛЯ ОРГАНИЗАЦИИ", extra={"body": callback.from_user.id})
    await state.update_data(profile_role="organizer")
    await callback.message.answer("Что вы хотите изменить?", reply_markup=edit_org_fields_keyboard())
    await callback.answer()


@router.callback_query(lambda c: c.data == "recreate_organization_profile")
async def recreate_organization(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(OrganizationProfileState.organization_name)
    await callback.message.answer("Название организации?")
    await callback.answer()


@router.callback_query(lambda c: c.data.startswith("edit_org_field_"))
async def choose_org_field(callback: CallbackQuery, state: FSMContext) -> None:
    field = callback.data.replace("edit_org_field_", "", 1)
    prompts = {
        "organization_name": "Введи новое название организации:",
        "representative_name": "Введи новое имя представителя:",
        "representative_phone": "Введи новый телефон представителя:",
        "website": "Введи новый сайт организации:",
        "description": "Введи новое описание организации:",
    }
    await state.set_state(EditOrganizationState.waiting_new_value)
    await state.update_data(edit_org_field=field)
    await callback.message.answer(prompts.get(field, "Введи новое значение:"))
    await callback.answer()


@router.message(EditOrganizationState.waiting_new_value)
async def update_organization_value(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    field = data.get("edit_org_field")
    value = (message.text or "").strip()
    if not field or not value:
        await message.answer("Введите корректное значение.")
        return

    if field == "representative_phone" and not re.fullmatch(r"^\+?\d{11}$", value):
        await message.answer("Телефон должен быть в формате 11 цифр или + и 11 цифр.")
        return

    result = await request_to_consumer(
        {
            "id": message.from_user.id,
            "action": "update_organization",
            "field": field,
            "value": value,
        }
    )
    if not result or "error" in result:
        await message.answer("Не удалось обновить профиль организации. Попробуй позже.")
        return

    await state.clear()
    await message.answer("Хочешь изменить что-то еще?", reply_markup=edit_org_more_keyboard())
    await state.update_data(profile_role="organizer")


@router.callback_query(lambda c: c.data == "edit_org_more_yes")
async def edit_org_more_yes(callback: CallbackQuery) -> None:
    await callback.message.answer("Что ты хочешь изменить?", reply_markup=edit_org_fields_keyboard())
    await callback.answer()


@router.callback_query(lambda c: c.data == "edit_org_more_no")
async def edit_org_more_no(callback: CallbackQuery) -> None:
    await callback.message.answer("Профиль обновлен!")
    await show_organization(callback)
