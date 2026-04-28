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


async def get_organization(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("ПОЛУЧЕН ЗАПРОС НА ПРОФИЛЬ ОРГАНИЗАЦИИ К БД", extra={"body": body.get("id")})
    user_id = body.get("id")
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            user_result = await db.execute(
                select(User).where(User.telegram_id == int(user_id))
            )
            user = user_result.scalar_one_or_none()
            if not user or user.role != "organizer":
                response_body = {"error": "organization_not_found"}
            else:
                org_result = await db.execute(
                    select(Organization).where(Organization.created_by == user.id)
                )
                organization = org_result.scalar_one_or_none()
                if not organization:
                    response_body = {"error": "organization_not_found"}
                else:
                    response_body = {
                        "organization_name": organization.name,
                        "representative_name": organization.representative_name,
                        "representative_phone": organization.representative_phone,
                        "website": organization.website,
                        "description": organization.description,
                    }
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ПОЛУЧЕНИЯ ПРОФИЛЯ ОРГАНИЗАЦИИ")
        response_body = {"error": "organization_fetch_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА ПРОФИЛЬ ОРГАНИЗАЦИИ", extra={"body": body.get("id")})