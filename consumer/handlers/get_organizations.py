import logging.config
from typing import Any, Dict

import aio_pika
import msgpack
from aio_pika import ExchangeType
from sqlalchemy import or_, select
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
        filters = body.get("filters") or {}
        cities = [c for c in filters.get("cities", []) if c]
        directions = [d for d in filters.get("directions", []) if d]
        types = [t for t in filters.get("types", []) if t]

        async with async_session() as db:
            logger.info("ПОЛУЧЕН ЗАПРОС НА ПОЛУЧЕНИЕ СПИСКА ОРГАНИЗАЦИЙ В БД")
            query = select(Organization)
            if cities:
                query = query.where(Organization.city.in_(cities))
            if directions:
                query = query.where(
                    or_(*[Organization.direction.ilike(f"%{direction}%") for direction in directions])
                )
            if types:
                query = query.where(Organization.type_organization.in_(types))
            query = query.order_by(Organization.id)

            result = await db.execute(query)
            organizations = result.scalars().all()
            response_body = {
                "organizations": [
                    {
                        "id": org.id,
                        "organization_name": org.name,
                        "city": org.city,
                        "direction": org.direction,
                        "type_organization": org.type_organization,
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
