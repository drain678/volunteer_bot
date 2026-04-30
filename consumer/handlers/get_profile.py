from datetime import datetime, timedelta

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
from src.models.models import Event, Participation, User

MOSCOW_OFFSET_HOURS = 3


async def get_profile(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("ПОЛУЧЕН ЗАПРОС НА ПРОФИЛЬ ВОЛОНТЕРА К БД", extra={"body": body.get("id")})

    user_id = body.get("id")
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            result = await db.execute(
                select(User).where(User.telegram_id == int(user_id))
            )
            user = result.scalar_one_or_none()

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

                if user.role == "volunteer":
                    completed_result = await db.execute(
                        select(Event.duration_hours)
                        .join(Participation, Participation.event_id == Event.id)
                        .where(
                            Participation.user_id == user.id,
                            Participation.status == "approved",
                            Event.is_finished.is_(True),
                        )
                    )
                    completed_rows = completed_result.all()
                    user.visited_events_count = len(completed_rows)
                    user.hours_total = float(sum(row[0] for row in completed_rows))
                    user.rating = user.hours_total * user.visited_events_count
                    await db.commit()

                response_body = {
                    "id": user.id,
                    "telegram_id": user.telegram_id,
                    "name": user.name,
                    "age": user.age,
                    "gender": user.gender,
                    "city": user.city,
                    "phone": user.phone,
                    "role": user.role,
                    "profile_filled": user.profile_filled,
                    "visited_events_count": user.visited_events_count,
                    "hours_total": user.hours_total,
                    "rating": user.rating,
                }

    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ПОЛУЧЕНИЯ ПРОФИЛЯ ВОЛОНТЕРА", extra={"body": body.get("id")})
        response_body = {"error": "profile_fetch_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )

        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА ПРОФИЛЬ ВОЛОНТЕРА", extra={"body": body.get("id")})

