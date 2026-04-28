import logging.config
from typing import Any, Dict

import aio_pika
import msgpack
from aio_pika import ExchangeType
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from config.settings import settings
from consumer.logger import LOGGING_CONFIG, logger
from consumer.storage import rabbit
from consumer.storage.db import async_session
from src.models.models import Organization, User


async def update_organization(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = int(body.get("id"))
    field = body.get("field")
    value = body.get("value")

    response_body: Dict[str, Any] | None = None
    try:
        async with async_session() as db:
            user_result = await db.execute(select(User).where(User.telegram_id == user_id))
            user = user_result.scalar_one_or_none()
            if not user:
                response_body = {"error": "user_not_found"}
            else:
                org_result = await db.execute(
                    select(Organization).where(Organization.created_by == user.id)
                )
                organization = org_result.scalar_one_or_none()
                if not organization:
                    response_body = {"error": "organization_not_found"}
                else:
                    if field == "organization_name":
                        organization.name = value
                    elif field in {
                        "representative_name",
                        "representative_phone",
                        "website",
                        "description",
                    }:
                        setattr(organization, field, value)
                        if field == "representative_phone":
                            user.phone = value
                        logger.info("ПРОФИЛЬ ОРГАНИЗАЦИИ ОБНОВЛЕН", extra={"body": body.get("id")})
                    else:
                        response_body = {"error": "invalid_field"}

                if response_body is None:
                    await db.commit()
                    response_body = {"status": "updated"}
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ОБНОВЛЕНИЯ ПРОФИЛЯ ОРГАНИЗАЦИИ")
        response_body = {"error": "db_error"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА ОБНОВЛЕНИЕ ПРОФИЛЯ ОРГАНИЗАЦИИ", extra={"body": body.get("id")})