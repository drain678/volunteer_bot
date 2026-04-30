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
from src.models.models import Event, Participation, User

MOSCOW_OFFSET_HOURS = 3


async def get_tops(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = body.get("id")
    response_body: Dict[str, Any]

    try:
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

            users_result = await db.execute(
                select(User).where(User.role == "volunteer").order_by(User.id)
            )
            volunteers = users_result.scalars().all()
            tops = []
            for volunteer in volunteers:
                completed_result = await db.execute(
                    select(Event.duration_hours)
                    .join(Participation, Participation.event_id == Event.id)
                    .where(
                        Participation.user_id == volunteer.id,
                        Participation.status == "approved",
                        Event.is_finished.is_(True),
                    )
                )
                completed_rows = completed_result.all()
                events_count = len(completed_rows)
                hours_total = float(sum(row[0] for row in completed_rows))
                rating = hours_total * events_count
                volunteer.visited_events_count = events_count
                volunteer.hours_total = hours_total
                volunteer.rating = rating
                tops.append(
                    {
                        "name": volunteer.name or "Без имени",
                        "rating": rating,
                    }
                )

            await db.commit()
            tops.sort(key=lambda item: item["rating"], reverse=True)
            response_body = {"tops": tops[:10]}
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ПОЛУЧЕНИЯ ТОПОВ ВОЛОНТЕРОВ")
        response_body = {"error": "tops_fetch_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
