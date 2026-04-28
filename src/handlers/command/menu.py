from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from sqlalchemy import select

from src.handlers.command.router import router
from src.models.models import User
from src.storage.db import async_session


def build_menu_by_role(role: str) -> InlineKeyboardMarkup:
    if role == "organizer":
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Организации", callback_data="organizations"),
                    InlineKeyboardButton(
                        text="Создать мероприятие", callback_data="create_event"
                    ),
                ],
                [   
                    InlineKeyboardButton(
                        text="Мои мероприятия", callback_data="my_events"
                    ),
                    InlineKeyboardButton(
                        text="Моя организация", callback_data="my_organization"
                    ),
                ],
            ]
        )

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Организации", callback_data="organizations"),
                InlineKeyboardButton(text="Мероприятия", callback_data="events"),
            ],
            [
                InlineKeyboardButton(text="Топы", callback_data="tops"),
                InlineKeyboardButton(text="Профиль", callback_data="profile"),
            ],
        ]
    )


@router.message(Command("menu"))
async def menu(message: Message) -> None:
    async with async_session() as db:
        result = await db.execute(
            select(User).where(User.telegram_id == message.from_user.id)
        )
        user = result.scalar_one_or_none()

    if not user:
        await message.answer("Сначала зарегистрируйся через /start")
        return

    keyboard = build_menu_by_role(user.role)

    await message.answer("Меню бота:", reply_markup=keyboard)
