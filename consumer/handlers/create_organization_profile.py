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


async def create_organization_profile(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("ЗАПРОС НА СОЗДАНИЕ ПРОФИЛЯ ОРГАНИЗАЦИИ")

    user_id = int(body.get("id"))
    representative_name = (body.get("representative_name") or "").strip()
    representative_phone = (body.get("representative_phone") or "").strip()
    website = (body.get("website") or "").strip()
    description = (body.get("description") or "").strip()

    if not all([representative_name, representative_phone, website, description]):
        response_body = {"error": "invalid_profile_data"}
    else:
        try:
            async with async_session() as db:
                user_result = await db.execute(
                    select(User).where(User.telegram_id == user_id)
                )
                user = user_result.scalar_one_or_none()
                if user is None:
                    user = User(
                        telegram_id=user_id,
                        role="organizer",
                        name=representative_name,
                        # Пока возраст для организатора не спрашиваем, сохраняем безопасное значение.
                        age=18,
                        profile_filled=True,
                    )
                    db.add(user)
                    await db.flush()
                else:
                    user.role = "organizer"
                    user.name = representative_name
                    user.profile_filled = True

                org_result = await db.execute(
                    select(Organization).where(Organization.created_by == user.id)
                )
                organization = org_result.scalar_one_or_none()
                if organization is None:
                    organization = Organization(
                        name=f"organization_{user.telegram_id}",
                        description=description,
                        representative_name=representative_name,
                        representative_phone=representative_phone,
                        website=website,
                        created_by=user.id,
                    )
                    db.add(organization)
                else:
                    organization.description = description
                    organization.representative_name = representative_name
                    organization.representative_phone = representative_phone
                    organization.website = website

                await db.commit()

                response_body = {
                    "role": user.role,
                    "representative_name": organization.representative_name,
                    "representative_phone": organization.representative_phone,
                    "website": organization.website,
                    "description": organization.description,
                }
        except SQLAlchemyError:
            logger.exception("ОШИБКА ПРИ СОЗДАНИИ ПРОФИЛЯ ОРГАНИЗАЦИИ")
            response_body = {"error": "db_error"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )

    logger.info(
        "БД СДЕЛАЛО ПРОФИЛЬ ОРГАНИЗАЦИИ",
        extra={"response": response_body},
    )
