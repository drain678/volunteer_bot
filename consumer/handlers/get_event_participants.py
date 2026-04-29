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


async def get_event_participants(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = body.get("id")
    event_id = body.get("event_id")
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            logger.info("ПОЛУЧЕН ЗАПРОС НА ПОЛУЧЕНИЕ УЧАСТНИКОВ МЕРОПРИЯТИЯ В БД", extra={"body": event_id})
            event_result = await db.execute(select(Event).where(Event.id == int(event_id)))
            event = event_result.scalar_one_or_none()
            if not event:
                response_body = {"error": "event_not_found"}
            else:
                participants_result = await db.execute(
                    select(User.name, User.phone, Participation.status)
                    .join(Participation, Participation.user_id == User.id)
                    .where(Participation.event_id == event.id)
                    .order_by(Participation.id)
                )
                participants_rows = participants_result.all()
                response_body = {
                    "event_title": event.title,
                    "participants": [
                        {"name": name, "phone": phone, "status": status}
                        for name, phone, status in participants_rows
                    ],
                }
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ПОЛУЧЕНИЯ УЧАСТНИКОВ МЕРОПРИЯТИЯ")
        response_body = {"error": "event_participants_fetch_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА ПОЛУЧЕНИЕ УЧАСТНИКОВ МЕРОПРИЯТИЯ", extra={"body": event_id})
