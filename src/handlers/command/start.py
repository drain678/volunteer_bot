from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from sqlalchemy.ext.asyncio import AsyncSession

from src.handlers.command.router import router
from src.templates.env import render
# from src.keyboards import volunteer_menu
from storage.db import get_db
from src.handlers.callback.get_profile import get_profile
from sqlalchemy import select
from src.models.models import User


@router.message(Command("start"))
async def start(message: Message, state: FSMContext, db: AsyncSession):
    await state.clear()

    result = await db.execute(
        select(User).where(User.telegram_id == message.from_user.id)
    )
    user = result.scalar_one_or_none()

    if user:
        if user.role == "volunteer":
            await message.answer(
                "Ты уже зарегистрирован как волонтёр",
                # reply_markup=volunteer_menu(),
            )
            return
        elif user.role == "organizer":
            await message.answer(
                "Ты уже зарегистрирован как организатор",
                # reply_markup=volunteer_menu(),
            )
            return
        else:
            await message.answer(
                "Ты уже зарегистрирован как администратор",
                # reply_markup=volunteer_menu(),
            )
            return

    keyboard = [
        [InlineKeyboardButton(text="Я волонтёр", callback_data="role_volunteer")],
        [InlineKeyboardButton(text="Я организатор", callback_data="role_organizer")],
    ]

    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    await message.answer(
        render("start.jinja2", user=message.from_user),
        reply_markup=reply_markup,
    )
