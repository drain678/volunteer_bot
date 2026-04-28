import logging.config
from typing import Any, Dict

import aio_pika
import msgpack
from aio_pika import ExchangeType
from sqlalchemy import delete, select
from sqlalchemy.exc import SQLAlchemyError

from config.settings import settings
from consumer.logger import LOGGING_CONFIG, logger
from consumer.storage import rabbit
from consumer.storage.db import async_session
from src.models.models import Event, Organization, User


async def delete_organization(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = int(body.get("id"))
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            user_result = await db.execute(select(User).where(User.telegram_id == user_id))
            user = user_result.scalar_one_or_none()
            if not user or user.role != "organizer":
                response_body = {"error": "organization_not_found"}
            else:
                await db.execute(delete(Event).where(Event.created_by == user.id))
                await db.execute(delete(Organization).where(Organization.created_by == user.id))
                user.role = "volunteer"
                user.profile_filled = False
                await db.commit()
                response_body = {"status": "deleted"}
                logger.info("ПРОФИЛЬ ОРГАНИЗАЦИИ УДАЛЕН", extra={"body": body.get("id")})
    except SQLAlchemyError:
        logger.exception("ОШИБКА УДАЛЕНИЯ ПРОФИЛЯ ОРГАНИЗАЦИИ")
        response_body = {"error": "db_error"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА УДАЛЕНИЕ ПРОФИЛЯ ОРГАНИЗАЦИИ", extra={"body": body.get("id")})
