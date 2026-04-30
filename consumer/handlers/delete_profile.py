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
from src.models.models import Event, Organization, Participation, User


async def delete_profile(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = int(body.get("id"))
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            result = await db.execute(select(User).where(User.telegram_id == user_id))
            user = result.scalar_one_or_none()

            if not user:
                response_body = {"error": "user_not_found"}
            else:
                await db.execute(delete(Participation).where(Participation.user_id == user.id))
                await db.execute(delete(Event).where(Event.created_by == user.id))
                await db.execute(delete(Organization).where(Organization.created_by == user.id))
                await db.delete(user)
                await db.commit()
                response_body = {"status": "deleted"}
                logger.info("ПРОФИЛЬ ВОЛОНТЕРА УДАЛЕН", extra={"body": body.get("id")})
    except SQLAlchemyError:
        logger.exception("ОШИБКА УДАЛЕНИЯ ПРОФИЛЯ")
        response_body = {"error": "db_error"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА УДАЛЕНИЕ ПРОФИЛЯ ВОЛОНТЕРА", extra={"body": body.get("id")})
