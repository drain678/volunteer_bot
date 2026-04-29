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
from src.models.models import Event, Organization, User


async def get_my_events(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = body.get("id")
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            logger.info("ПОЛУЧЕН ЗАПРОС НА ПОЛУЧЕНИЕ МОИХ МЕРОПРИЯТИЙ В БД", extra={"body": user_id})
            user_result = await db.execute(select(User).where(User.telegram_id == int(user_id)))
            user = user_result.scalar_one_or_none()
            if not user or user.role != "organizer":
                response_body = {"error": "organizer_not_found"}
            else:
                org_result = await db.execute(
                    select(Organization).where(Organization.created_by == user.id)
                )
                organization = org_result.scalar_one_or_none()
                if not organization:
                    response_body = {"events": []}
                else:
                    events_result = await db.execute(
                        select(Event)
                        .where(Event.organization_id == organization.id)
                        .order_by(Event.id)
                    )
                    events = events_result.scalars().all()
                    response_body = {
                        "events": [
                            {
                                "id": event.id,
                                "title": event.title,
                                "description": event.description,
                                "min_age": event.min_age,
                                "city": event.city,
                                "direction": event.direction,
                                "start_time": event.start_time.strftime("%d.%m.%Y %H:%M"),
                                "duration_hours": event.duration_hours,
                            }
                            for event in events
                        ]
                    }
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ПОЛУЧЕНИЯ МОИХ МЕРОПРИЯТИЙ")
        response_body = {"error": "my_events_fetch_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА ПОЛУЧЕНИЕ МОИХ МЕРОПРИЯТИЙ", extra={"body": user_id})

