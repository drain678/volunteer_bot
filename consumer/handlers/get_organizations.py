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
from src.models.models import Organization


async def get_organizations(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = body.get("id")
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            result = await db.execute(select(Organization).order_by(Organization.id))
            organizations = result.scalars().all()
            response_body = {
                "organizations": [
                    {
                        "organization_name": org.name,
                        "representative_name": org.representative_name,
                        "representative_phone": org.representative_phone,
                        "website": org.website,
                        "description": org.description,
                    }
                    for org in organizations
                ]
            }
    except SQLAlchemyError:
        logger.exception("ОШИБКА ПОЛУЧЕНИЯ СПИСКА ОРГАНИЗАЦИЙ")
        response_body = {"error": "organizations_fetch_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА СПИСОК ОРГАНИЗАЦИЙ", extra={"body": body.get("id")})
