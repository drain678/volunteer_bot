from aiogram.filters import Command
from aiogram.filters.command import CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from sqlalchemy import select

from src.handlers.callback.get_events import send_event_card
from src.handlers.callback.get_my_events import send_my_event_card
from src.handlers.callback.get_organizations import send_organization_profile
from src.handlers.callback.get_volunteer_my_events import send_volunteer_event_card
from src.handlers.command.menu import build_menu_by_role
from src.handlers.command.router import router
from src.models.models import User
from src.storage.db import async_session
from src.templates.env import render


@router.message(Command("start"))
async def start(message: Message, state: FSMContext, command: CommandObject | None = None) -> None:
    await state.set_state(None)

    deep_link_arg = (command.args or "").strip() if command else ""
    if deep_link_arg.startswith("org_"):
        parts = deep_link_arg.split("_")
        if len(parts) == 3:
            try:
                organization_id = int(parts[1])
                page = int(parts[2])
            except ValueError:
                await message.answer("Некорректная ссылка на организацию.")
                return
            opened = await send_organization_profile(
                message=message,
                user_id=message.from_user.id,
                organization_id=organization_id,
                page=page,
            )
            if opened:
                return
    if deep_link_arg.startswith("vmy_event_"):
        parts = deep_link_arg.split("_")
        if len(parts) == 5:
            try:
                kind = parts[2]
                event_id = int(parts[3])
                page = int(parts[4])
            except ValueError:
                await message.answer("Некорректная ссылка на мероприятие.")
                return
            opened = await send_volunteer_event_card(
                message=message,
                user_id=message.from_user.id,
                kind=kind,
                event_id=event_id,
                page=page,
            )
            if opened:
                return
    if deep_link_arg.startswith("event_"):
        parts = deep_link_arg.split("_")
        if len(parts) == 3:
            try:
                event_id = int(parts[1])
                page = int(parts[2])
            except ValueError:
                await message.answer("Некорректная ссылка на мероприятие.")
                return
            opened = await send_event_card(
                message=message,
                user_id=message.from_user.id,
                event_id=event_id,
                page=page,
            )
            if opened:
                return
    if deep_link_arg.startswith("my_event_"):
        parts = deep_link_arg.split("_")
        if len(parts) == 4:
            try:
                event_id = int(parts[2])
                page = int(parts[3])
            except ValueError:
                await message.answer("Некорректная ссылка на мероприятие.")
                return
            opened = await send_my_event_card(
                message=message,
                user_id=message.from_user.id,
                event_id=event_id,
                page=page,
            )
            if opened:
                return

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