import logging.config
from datetime import datetime, time, timedelta
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
from src.models.models import Event, Organization

MOSCOW_OFFSET_HOURS = 3


async def get_events(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = body.get("id")
    response_body: Dict[str, Any]

    try:
        filters = body.get("filters") or {}
        cities = [c for c in filters.get("cities", []) if c]
        directions = [d for d in filters.get("directions", []) if d]
        date_from_text = (filters.get("date_from") or "").strip()
        date_to_text = (filters.get("date_to") or "").strip()
        organization_id = filters.get("organization_id")

        async with async_session() as db:
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
                .join(Organization, Event.organization_id == Organization.id)
                .where(Event.is_finished.is_(False))
                .order_by(Event.start_time, Event.id)
            )
            if cities:
                query = query.where(Event.city.in_(cities))
            if directions:
                query = query.where(
                    or_(*[Event.direction.ilike(f"%{direction}%") for direction in directions])
                )
            if organization_id:
                query = query.where(Event.organization_id == int(organization_id))
            if date_from_text and date_to_text:
                date_from = datetime.strptime(date_from_text, "%d.%m.%Y").date()
                date_to = datetime.strptime(date_to_text, "%d.%m.%Y").date()
                if date_to < date_from:
                    date_from, date_to = date_to, date_from

                dt_from = datetime.combine(date_from, time.min)
                dt_to = datetime.combine(date_to + timedelta(days=1), time.min)
                query = query.where(Event.start_time >= dt_from, Event.start_time < dt_to)

            result = await db.execute(query)
            rows = result.all()
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
                        "organization_name": organization_name,
                    }
                    for event, organization_name in rows
                ]
            }
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ПОЛУЧЕНИЯ СПИСКА МЕРОПРИЯТИЙ")
        response_body = {"error": "events_fetch_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
