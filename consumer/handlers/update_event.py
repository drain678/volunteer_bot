import logging.config
from datetime import datetime
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


async def update_event(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = int(body.get("id"))
    event_id = body.get("event_id")
    updates: Dict[str, Any] = dict(body.get("updates") or {})
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            user_result = await db.execute(select(User).where(User.telegram_id == user_id))
            user = user_result.scalar_one_or_none()
            if not user:
                response_body = {"error": "user_not_found"}
            else:
                event_result = await db.execute(
                    select(Event).where(Event.id == int(event_id), Event.created_by == user.id)
                )
                event = event_result.scalar_one_or_none()
                if not event:
                    response_body = {"error": "event_not_found"}
                elif not updates:
                    response_body = {"error": "empty_updates"}
                else:
                    old_event_title = event.title
                    if "title" in updates:
                        event.title = str(updates["title"]).strip()
                    if "description" in updates:
                        event.description = str(updates["description"]).strip()
                    if "direction" in updates:
                        event.direction = str(updates["direction"]).strip()
                    if "city" in updates:
                        event.city = str(updates["city"]).strip()
                    if "duration_hours" in updates:
                        event.duration_hours = float(updates["duration_hours"])
                    if "min_age" in updates:
                        event.min_age = int(updates["min_age"])

                    start_date = updates.get("start_date")
                    start_time = updates.get("start_time")
                    if start_date or start_time:
                        date_part = start_date or event.start_time.strftime("%d.%m.%Y")
                        time_part = start_time or event.start_time.strftime("%H:%M")
                        event.start_time = datetime.strptime(f"{date_part} {time_part}", "%d.%m.%Y %H:%M")

                    await db.commit()

                    participants_result = await db.execute(
                        select(User.telegram_id)
                        .join(Participation, Participation.user_id == User.id)
                        .where(Participation.event_id == event.id)
                    )
                    volunteer_ids = [row[0] for row in participants_result.all()]
                    response_body = {
                        "status": "updated",
                        "event_title": event.title,
                        "old_event_title": old_event_title,
                        "volunteer_telegram_ids": volunteer_ids,
                    }
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ОБНОВЛЕНИЯ МЕРОПРИЯТИЯ")
        response_body = {"error": "update_event_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА ОБНОВЛЕНИЕ МЕРОПРИЯТИЯ", extra={"body": user_id})
