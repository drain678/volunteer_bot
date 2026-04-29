from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from sqlalchemy import select

from src.handlers.command.menu import build_menu_by_role
from src.handlers.command.router import router
from src.models.models import User
from src.storage.db import async_session


def _test_role_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Волонтер", callback_data="test_role_volunteer"),
                InlineKeyboardButton(text="Организатор", callback_data="test_role_organizer"),
            ],
            [InlineKeyboardButton(text="Администратор", callback_data="test_role_admin")],
        ]
    )


async def _get_user(telegram_id: int) -> User | None:
    async with async_session() as db:
        result = await db.execute(select(User).where(User.telegram_id == telegram_id))
        return result.scalar_one_or_none()


@router.message(Command("test_role"))
async def test_role(message: Message, state: FSMContext) -> None:
    user = await _get_user(message.from_user.id)
    if not user:
        await message.answer("Сначала зарегистрируйся через /start")
        return

    data = await state.get_data()
    real_role = data.get("admin_real_role")
    if user.role != "admin" and real_role != "admin":
        await message.answer("Эта команда доступна только администратору.")
        return

    if user.role == "admin" and real_role != "admin":
        await state.update_data(admin_real_role="admin")

    await message.answer("Выбери тестовую роль:", reply_markup=_test_role_keyboard())


@router.callback_query(lambda c: c.data.startswith("test_role_"))
async def set_test_role(callback: CallbackQuery, state: FSMContext) -> None:
    role = callback.data.removeprefix("test_role_")
    if role not in {"volunteer", "organizer", "admin"}:
        await callback.answer("Некорректная роль", show_alert=True)
        return

    user = await _get_user(callback.from_user.id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return

    data = await state.get_data()
    real_role = data.get("admin_real_role")
    if user.role != "admin" and real_role != "admin":
        await callback.answer("Доступ только для администратора", show_alert=True)
        return

    async with async_session() as db:
        result = await db.execute(select(User).where(User.telegram_id == callback.from_user.id))
        db_user = result.scalar_one_or_none()
        if not db_user:
            await callback.answer("Пользователь не найден", show_alert=True)
            return
        db_user.role = role
        await db.commit()

    await callback.message.answer(
        f"Тестовая роль установлена: {role}", reply_markup=build_menu_by_role(role)
    )
    await callback.answer()


@router.message(Command("reset_role"))
async def reset_test_role(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    real_role = data.get("admin_real_role")
    if real_role != "admin":
        await message.answer("Нет активного тестового режима или вы не администратор.")
        return

    async with async_session() as db:
        result = await db.execute(select(User).where(User.telegram_id == message.from_user.id))
        user = result.scalar_one_or_none()
        if not user:
            await message.answer("Пользователь не найден.")
            return
        user.role = "admin"
        await db.commit()

    await state.update_data(admin_real_role=None)
    await message.answer("Роль возвращена на администратор.", reply_markup=build_menu_by_role("admin"))
