import logging.config
from datetime import datetime, timedelta
from typing import Any, Dict

import aio_pika
import msgpack
from aio_pika import ExchangeType
from sqlalchemy import delete, select
from sqlalchemy.exc import SQLAlchemyError

from config.settings import settings
from consumer.logger import LOGGING_CONFIG, logger
from consumer.storage import rabbit
from consumer.storage.db import async_session
from src.models.models import Event, Organization, Participation, User

MOSCOW_OFFSET_HOURS = 3


async def delete_event(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = body.get("id")
    event_id = body.get("event_id")
    reason = (body.get("reason") or "").strip()
    response_body: Dict[str, Any]

    try:
        async with async_session() as db:
            logger.info(f"ПОЛУЧЕН ЗАПРОС НА УДАЛЕНИЕ МЕРОПРИЯТИЯ")
            user_result = await db.execute(select(User).where(User.telegram_id == int(user_id)))
            user = user_result.scalar_one_or_none()
            if not user or user.role != "organizer":
                response_body = {"error": "organizer_not_found"}
            else:
                event_result = await db.execute(select(Event).where(Event.id == int(event_id)))
                event = event_result.scalar_one_or_none()
                if not event:
                    response_body = {"error": "event_not_found"}
                else:
                    org_result = await db.execute(
                        select(Organization).where(Organization.id == event.organization_id)
                    )
                    org = org_result.scalar_one_or_none()
                    if not org or org.created_by != user.id:
                        response_body = {"error": "access_denied"}
                    else:
                        now = datetime.utcnow() + timedelta(hours=MOSCOW_OFFSET_HOURS)
                        requires_reason = event.start_time > now
                        if requires_reason and not reason:
                            response_body = {"error": "reason_required"}
                        else:
                            approved_result = await db.execute(
                                select(User.telegram_id)
                                .join(Participation, Participation.user_id == User.id)
                                .where(
                                    Participation.event_id == event.id,
                                    Participation.status == "approved",
                                )
                            )
                            volunteer_ids = [row[0] for row in approved_result.all() if row[0]]
                            event_title = event.title
                            await db.execute(
                                delete(Participation).where(Participation.event_id == event.id)
                            )
                            await db.delete(event)
                            await db.commit()
                            response_body = {
                                "ok": True,
                                "event_title": event_title,
                                "reason": reason,
                                "volunteer_telegram_ids": volunteer_ids,
                            }
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА УДАЛЕНИЯ МЕРОПРИЯТИЯ")
        response_body = {"error": "delete_event_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
