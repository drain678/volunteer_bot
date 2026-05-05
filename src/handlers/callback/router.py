from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware, Router
from aiogram.types import CallbackQuery
from sqlalchemy import select

from src.models.models import Organization, User
from src.storage.db import async_session

router = Router()

_BANNED_ORGANIZER_CALLBACK_PREFIXES = (
    "organizations",
    "org_",
    "organization_",
    "create_event",
    "event_direction_",
    "my_events",
    "my_event_",
    "my_organization",
    "edit_organization",
    "recreate_organization_profile",
    "edit_org_",
    "delete_organization",
)


class BannedOrganizerMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        callback_data = event.data or ""
        if not callback_data.startswith(_BANNED_ORGANIZER_CALLBACK_PREFIXES):
            return await handler(event, data)

        async with async_session() as db:
            user_result = await db.execute(
                select(User).where(User.telegram_id == event.from_user.id)
            )
            user = user_result.scalar_one_or_none()
            if not user or user.role != "organizer":
                return await handler(event, data)

            org_result = await db.execute(
                select(Organization).where(Organization.created_by == user.id)
            )
            organization = org_result.scalar_one_or_none()
            if organization and organization.is_banned:
                await event.answer(
                    "Ваша организация заблокирована. Команды организатора недоступны.",
                    show_alert=True,
                )
                return None
        return await handler(event, data)


router.callback_query.middleware(BannedOrganizerMiddleware())
