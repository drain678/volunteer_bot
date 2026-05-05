from aiogram.filters import Command
from aiogram.filters.command import CommandObject
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import Message
from sqlalchemy import select

from src.handlers.command.router import router
from src.handlers.state.organization_ban import OrganizationBanState
from src.models.models import Organization, User
from src.storage.db import async_session


async def _is_admin(telegram_id: int) -> bool:
    async with async_session() as db:
        result = await db.execute(select(User).where(User.telegram_id == telegram_id))
        user = result.scalar_one_or_none()
        return bool(user and (user.is_admin or user.role == "admin"))


@router.message(Command("ban"))
async def ban_organization_start(message: Message, state: FSMContext, command: CommandObject | None = None) -> None:
    if not await _is_admin(message.from_user.id):
        await message.answer("Команда доступна только администратору.")
        return

    raw = (command.args or "").strip() if command else ""
    if not raw or not raw.isdigit():
        await message.answer("Использование: /ban <id организации>")
        return
    organization_id = int(raw)
    async with async_session() as db:
        result = await db.execute(select(Organization).where(Organization.id == organization_id))
        organization = result.scalar_one_or_none()
        if not organization:
            await message.answer("Организация не найдена.")
            return
        if organization.is_banned:
            await message.answer("Эта организация уже забанена.")
            return
    await state.set_state(OrganizationBanState.waiting_reason)
    await state.update_data(ban_organization_id=organization_id)
    await message.answer("Укажите причину бана:")


@router.message(OrganizationBanState.waiting_reason)
async def ban_organization_reason(message: Message, state: FSMContext) -> None:
    if not await _is_admin(message.from_user.id):
        await message.answer("Команда доступна только администратору.")
        await state.clear()
        return

    reason = (message.text or "").strip()
    if not reason:
        await message.answer("Причина не должна быть пустой.")
        return

    data = await state.get_data()
    organization_id = data.get("ban_organization_id")
    if not organization_id:
        await message.answer("Не удалось определить организацию.")
        await state.clear()
        return

    organizer_tg: int | None = None
    organization_name = "организация"
    async with async_session() as db:
        org_result = await db.execute(select(Organization).where(Organization.id == int(organization_id)))
        organization = org_result.scalar_one_or_none()
        if not organization:
            await message.answer("Организация не найдена.")
            await state.clear()
            return
        organization.is_banned = True
        organization_name = organization.name or organization_name

        user_result = await db.execute(select(User).where(User.id == organization.created_by))
        organizer = user_result.scalar_one_or_none()
        if organizer:
            organizer_tg = organizer.telegram_id

        await db.commit()

    if organizer_tg:
        try:
            await message.bot.send_message(
                organizer_tg,
                f"Ваша организация «{organization_name}» была забанена.\nПричина: {reason}",
            )
        except TelegramBadRequest:
            pass
    await message.answer(f"Организация {organization_id} забанена.")
    await state.clear()


@router.message(Command("unban"))
async def unban_organization(message: Message, command: CommandObject | None = None) -> None:
    if not await _is_admin(message.from_user.id):
        await message.answer("Команда доступна только администратору.")
        return

    raw = (command.args or "").strip() if command else ""
    if not raw or not raw.isdigit():
        await message.answer("Использование: /unban <id организации>")
        return
    organization_id = int(raw)

    organizer_tg: int | None = None
    organization_name = "организация"
    async with async_session() as db:
        org_result = await db.execute(select(Organization).where(Organization.id == organization_id))
        organization = org_result.scalar_one_or_none()
        if not organization:
            await message.answer("Организация не найдена.")
            return
        if not organization.is_banned:
            await message.answer("Эта организация уже разбанена.")
            return
        organization.is_banned = False
        organization_name = organization.name or organization_name

        user_result = await db.execute(select(User).where(User.id == organization.created_by))
        organizer = user_result.scalar_one_or_none()
        if organizer:
            organizer_tg = organizer.telegram_id

        await db.commit()

    if organizer_tg:
        try:
            await message.bot.send_message(
                organizer_tg,
                f"Ваша организация «{organization_name}» была разбанена.",
            )
        except TelegramBadRequest:
            pass
    await message.answer(f"Организация {organization_id} разбанена.")
