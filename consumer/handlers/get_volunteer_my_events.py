import logging.config
from datetime import datetime, timedelta
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
from src.models.models import Event, Organization, Participation, User

MOSCOW_OFFSET_HOURS = 3


async def get_volunteer_my_events(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = body.get("id")
    kind = body.get("kind") or "upcoming"
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            logger.info(f"ПОЛУЧЕН ЗАПРОС НА МОИ МЕРОПРИЯТИЯ ВОЛОНТЕРА {user_id}")
            user_result = await db.execute(select(User).where(User.telegram_id == int(user_id)))
            user = user_result.scalar_one_or_none()
            if not user:
                response_body = {"error": "user_not_found"}
            else:
                now = datetime.utcnow() + timedelta(hours=MOSCOW_OFFSET_HOURS)
                outdated_result = await db.execute(
                    select(Event).where(Event.is_finished.is_(False), Event.start_time < now)
                )
                outdated = outdated_result.scalars().all()
                for item in outdated:
                    item.is_finished = True
                if outdated:
                    await db.commit()

                query = (
                    select(Event, Organization.name)
                    .join(Participation, Participation.event_id == Event.id)
                    .join(Organization, Organization.id == Event.organization_id)
                    .where(
                        Participation.user_id == user.id,
                        Participation.status == "approved",
                    )
                )
                if kind == "past":
                    query = query.where(Event.is_finished.is_(True))
                else:
                    query = query.where(Event.is_finished.is_(False))
                query = query.order_by(Event.start_time, Event.id)
                events_result = await db.execute(query)
                rows = events_result.all()
                response_body = {
                    "events": [
                        {
                            "id": event.id,
                            "title": event.title,
                            "organization_name": organization_name,
                            "description": event.description,
                            "min_age": event.min_age,
                            "city": event.city,
                            "direction": event.direction,
                            "start_time": event.start_time.strftime("%d.%m.%Y %H:%M"),
                            "duration_hours": event.duration_hours,
                            "is_finished": event.is_finished,
                        }
                        for event, organization_name in rows
                    ]
                }
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ПОЛУЧЕНИЯ МОИХ МЕРОПРИЯТИЙ ВОЛОНТЕРА")
        response_body = {"error": "volunteer_events_fetch_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
