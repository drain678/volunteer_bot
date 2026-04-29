from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from sqlalchemy import select

from src.handlers.command.menu import build_menu_by_role
from src.handlers.command.router import router
from src.models.models import User
from src.storage.db import async_session
from src.templates.env import render


@router.message(Command("start"))
async def start(message: Message, state: FSMContext) -> None:
    await state.set_state(None)

    async with async_session() as db:
        result = await db.execute(
            select(User).where(User.telegram_id == message.from_user.id)
        )
        user = result.scalar_one_or_none()

    if user:
        keyboard = None
        if user.role == "volunteer":
            ans = "Ты уже зарегистрирован как волонтёр"
        elif user.role == "organizer":
            ans = "Ты уже зарегистрирован как организатор"
        else:
            ans = "Ты уже зарегистрирован как администратор"
    else:
        ans = "Давай зарегистрируемся, кем ты будешь?"
        keyboard = [
            [InlineKeyboardButton(text="Я волонтёр", callback_data="role_volunteer")],
            [InlineKeyboardButton(text="Я организатор", callback_data="role_organizer")],
        ]

    if keyboard:
        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    else:
        reply_markup = None

    await message.answer(
        render("start.jinja2", user=message.from_user),
        # reply_markup=reply_markup,
    )

    if ans:
        await message.answer(ans, reply_markup=reply_markup)

    if user:
        await message.answer("Меню бота:", reply_markup=build_menu_by_role(user.role))