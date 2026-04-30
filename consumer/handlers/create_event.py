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
from src.models.models import Event, Organization, User


async def create_event(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = int(body.get("id"))
    response_body: Dict[str, Any]

    try:
        title = (body.get("title") or "").strip()
        description = (body.get("description") or "").strip()
        city = (body.get("city") or "").strip()
        direction = (body.get("direction") or "").strip()
        min_age = int(body.get("min_age"))
        duration_hours = float(body.get("duration_hours"))
        start_time = datetime.strptime(body.get("start_time"), "%d.%m.%Y %H:%M")

        if (
            not all([title, description, city, direction])
            or duration_hours <= 0
            or min_age < 14
            or min_age > 100
        ):
            response_body = {"error": "invalid_event_data"}
        else:
            async with async_session() as db:
                logger.info("ЗАПРОС НА СОЗДАНИЕ МЕРОПРИЯТИЯ В БД", extra={"body": user_id})
                user_result = await db.execute(
                    select(User).where(User.telegram_id == user_id)
                )
                user = user_result.scalar_one_or_none()
                if not user or user.role != "organizer":
                    response_body = {"error": "organizer_not_found"}
                else:
                    org_result = await db.execute(
                        select(Organization).where(Organization.created_by == user.id)
                    )
                    organization = org_result.scalar_one_or_none()
                    if not organization:
                        response_body = {"error": "organization_not_found"}
                    else:
                        event = Event(
                            title=title,
                            description=description,
                            min_age=min_age,
                            city=city,
                            direction=direction,
                            start_time=start_time,
                            duration_hours=duration_hours,
                            organization_id=organization.id,
                            created_by=user.id,
                        )
                        db.add(event)
                        await db.commit()
                        logger.info("БД СДЕЛАЛА МЕРОПРИЯТИЕ", extra={"body": user_id})

                        response_body = {
                            "title": event.title,
                            "description": event.description,
                            "min_age": event.min_age,
                            "city": event.city,
                            "direction": event.direction,
                            "start_time": event.start_time.strftime("%d.%m.%Y %H:%M"),
                            "duration_hours": event.duration_hours,
                        }
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА СОЗДАНИЯ МЕРОПРИЯТИЯ")
        response_body = {"error": "event_create_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА СОЗДАНИЕ МЕРОПРИЯТИЯ", extra={"body": user_id})
