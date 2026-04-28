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


async def get_profile(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("ПОЛУЧЕН ЗАПРОС НА ПРОФИЛЬ", extra={"body": body})

    user_id = body.get("id")
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            logger.info("ПОЛУЧЕН ЗАПРОС К БД")
            result = await db.execute(
                select(User).where(User.telegram_id == int(user_id))
            )
            user = result.scalar_one_or_none()

            if not user:
                response_body = {"error": "user_not_found"}
            else:
                response_body = {
                    "id": user.id,
                    "telegram_id": user.telegram_id,
                    "name": user.name,
                    "age": user.age,
                    "gender": user.gender,
                    "city": user.city,
                    "role": user.role,
                    "profile_filled": user.profile_filled,
                }

                if user.role == "organizer":
                    org_result = await db.execute(
                        select(Organization).where(Organization.created_by == user.id)
                    )
                    organization = org_result.scalar_one_or_none()
                    if organization:
                        response_body.update(
                            {
                                "organization_name": organization.name,
                                "representative_name": organization.representative_name,
                                "representative_phone": organization.representative_phone,
                                "website": organization.website,
                                "description": organization.description,
                            }
                        )
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ПОЛУЧЕНИЯ ПРОФИЛЯ")
        response_body = {"error": "profile_fetch_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )

        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )

    logger.info("Профиль отправлен", extra={"response": response_body})
