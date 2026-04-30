import logging.config
import json
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
                elif event.is_finished:
                    response_body = {"error": "event_finished"}
                elif user.age is None or user.age < event.min_age:
                    response_body = {
                        "error": "age_restriction",
                        "required_min_age": event.min_age,
                    }
                else:
                    existing_result = await db.execute(
                        select(Participation).where(
                            Participation.user_id == user.id,
                            Participation.event_id == event.id,
                        )
                    )
                    existing = existing_result.scalar_one_or_none()
                    profile_snapshot = json.dumps(
                        {
                            "name": user.name or "",
                            "age": user.age,
                            "city": user.city or "",
                            "phone": user.phone or "",
                            "gender": user.gender or "",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    if existing:
                        if existing.status == "approved":
                            response_body = {"error": "already_participating"}
                        elif (
                            existing.status == "rejected"
                            and existing.profile_snapshot == profile_snapshot
                        ):
                            response_body = {
                                "error": "already_rejected_same_profile",
                                "event_title": event.title,
                            }
                        else:
                            existing.status = "pending"
                            existing.profile_snapshot = profile_snapshot
                            await db.commit()
                            participation = existing
                            organizer_result = await db.execute(
                                select(User).where(User.id == event.created_by)
                            )
                            organizer = organizer_result.scalar_one_or_none()
                            response_body = {
                                "ok": True,
                                "participation_id": participation.id,
                                "event_title": event.title,
                                "organizer_telegram_id": organizer.telegram_id if organizer else None,
                                "volunteer": {
                                    "name": user.name,
                                    "age": user.age,
                                    "city": user.city,
                                    "phone": user.phone,
                                    "gender": user.gender,
                                },
                            }
                    else:
                        participation = Participation(
                            user_id=user.id,
                            event_id=event.id,
                            profile_snapshot=profile_snapshot,
                        )
                        db.add(participation)
                        await db.flush()
                        await db.commit()
                        organizer_result = await db.execute(
                            select(User).where(User.id == event.created_by)
                        )
                        organizer = organizer_result.scalar_one_or_none()
                        response_body = {
                            "ok": True,
                            "participation_id": participation.id,
                            "event_title": event.title,
                            "organizer_telegram_id": organizer.telegram_id if organizer else None,
                            "volunteer": {
                                "name": user.name,
                                "age": user.age,
                                "city": user.city,
                                "phone": user.phone,
                                "gender": user.gender,
                            },
                        }
    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ЗАПИСИ НА МЕРОПРИЯТИЕ")
        response_body = {"error": "participation_failed"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА ЗАПИСЬ НА МЕРОПРИЯТИЕ", extra={"body": user_id})
