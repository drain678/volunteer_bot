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


async def participate_event(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = body.get("id")
    event_id = body.get("event_id")
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            user_result = await db.execute(
                select(User).where(User.telegram_id == int(user_id))
            )
            user = user_result.scalar_one_or_none()
            if not user:
                response_body = {"error": "user_not_found"}
            else:
                event_result = await db.execute(select(Event).where(Event.id == int(event_id)))
                event = event_result.scalar_one_or_none()
                if not event:
                    response_body = {"error": "event_not_found"}
                else:
                    existing_result = await db.execute(
                        select(Participation).where(
                            Participation.user_id == user.id,
                            Participation.event_id == event.id,
                        )
                    )
                    existing = existing_result.scalar_one_or_none()
                    if existing:
                        response_body = {"error": "already_participating"}
                    else:
                        participation = Participation(user_id=user.id, event_id=event.id)
                        db.add(participation)
                        await db.commit()
                        response_body = {"ok": True}
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ЗАПИСИ НА МЕРОПРИЯТИЕ")
        response_body = {"error": "participation_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
