import logging.config
from typing import Any, Dict

import aio_pika
import msgpack
from aio_pika import ExchangeType
from sqlalchemy import select

from config.settings import settings
from consumer.logger import LOGGING_CONFIG, logger
from consumer.storage import rabbit
from consumer.storage.db import async_session
from src.models.models import User


async def get_profile(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("Получен запрос на профиль", extra={"body": body})

    user_id = body.get("id")

    async with async_session() as db:
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
                "role": user.role,
            }

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user", ExchangeType.TOPIC, durable=True
        )

        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )

    logger.info("Профиль отправлен", extra={"response": response_body})
