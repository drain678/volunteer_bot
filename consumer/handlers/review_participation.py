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


async def review_participation(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    organizer_tg_id = body.get("id")
    participation_id = body.get("participation_id")
    decision = body.get("decision")
    reason = (body.get("reason") or "").strip()
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            organizer_result = await db.execute(
                select(User).where(User.telegram_id == int(organizer_tg_id))
            )
            organizer = organizer_result.scalar_one_or_none()
            if not organizer:
                response_body = {"error": "organizer_not_found"}
            else:
                p_result = await db.execute(
                    select(Participation).where(Participation.id == int(participation_id))
                )
                participation = p_result.scalar_one_or_none()
                if not participation:
                    response_body = {"error": "participation_not_found"}
                else:
                    event_result = await db.execute(
                        select(Event).where(Event.id == participation.event_id)
                    )
                    event = event_result.scalar_one_or_none()
                    if not event or event.created_by != organizer.id:
                        response_body = {"error": "access_denied"}
                    else:
                        volunteer_result = await db.execute(
                            select(User).where(User.id == participation.user_id)
                        )
                        volunteer = volunteer_result.scalar_one_or_none()
                        if not volunteer:
                            response_body = {"error": "volunteer_not_found"}
                        else:
                            if decision == "approve":
                                participation.status = "approved"
                                await db.commit()
                                response_body = {
                                    "ok": True,
                                    "decision": "approve",
                                    "event_title": event.title,
                                    "volunteer_telegram_id": volunteer.telegram_id,
                                }
                            elif decision == "reject":
                                participation.status = "rejected"
                                await db.commit()
                                response_body = {
                                    "ok": True,
                                    "decision": "reject",
                                    "event_title": event.title,
                                    "reason": reason,
                                    "volunteer_telegram_id": volunteer.telegram_id,
                                }
                            else:
                                response_body = {"error": "invalid_decision"}
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ОБРАБОТКИ ЗАЯВКИ")
        response_body = {"error": "review_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=organizer_tg_id),
        )
