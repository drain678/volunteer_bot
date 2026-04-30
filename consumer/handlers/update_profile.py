import logging.config
import re
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
from src.models.models import User


def _normalize_phone(phone: str) -> str:
    phone = phone.strip()
    has_plus = phone.startswith("+")
    digits = "".join(ch for ch in phone if ch.isdigit())
    return f"+{digits}" if has_plus else digits


def _is_valid_phone(phone: str) -> bool:
    if phone.startswith("+"):
        return bool(re.fullmatch(r"^\+7\d{10}$", phone))
    return bool(re.fullmatch(r"^8\d{10}$", phone))


async def update_profile(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    user_id = int(body.get("id"))
    field = body.get("field")
    value = body.get("value")

    response_body: Dict[str, Any] | None = None
    try:
        async with async_session() as db:
            result = await db.execute(select(User).where(User.telegram_id == user_id))
            user = result.scalar_one_or_none()
            if not user:
                response_body = {"error": "user_not_found"}
            else:
                if field in {"name", "city", "gender", "phone"}:
                    if field == "phone":
                        value = _normalize_phone(str(value))
                        if not _is_valid_phone(value):
                            response_body = {"error": "invalid_phone"}
                    if field == "gender" and value not in {"f", "m"}:
                        response_body = {"error": "invalid_gender"}
                    if response_body is not None:
                        pass
                    else:
                        setattr(user, field, value)
                elif field == "age":
                    age_value = int(value)
                    if age_value < 14 or age_value > 100:
                        response_body = {"error": "invalid_age"}
                    else:
                        user.age = age_value
                else:
                    response_body = {"error": "invalid_field"}

                if response_body is None:
                    await db.commit()
                    response_body = {"status": "updated"}
                logger.info("ОБНОВЛЕНИЕ ПРОФИЛЯ ВОЛОНТЕРА В БД", extra={"body": body.get("id")})

    except (SQLAlchemyError, ValueError, TypeError):
        logger.exception("ОШИБКА ОБНОВЛЕНИЯ ПРОФИЛЯ")
        response_body = {"error": "db_error"}

    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )
        logger.info("ОТПРАВИЛИ ОТВЕТ НА ОБНОВЛЕНИЕ ПРОФИЛЯ ВОЛОНТЕРА", extra={"body": body.get("id")})
