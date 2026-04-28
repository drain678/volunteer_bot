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
from src.models.models import User


async def create_profile(body: Dict[str, Any]) -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("ПОЛУЧИЛИ ЗАПРОС НА СОЗДАНИЕ ПРОФИЛЯ К БД", extra={"body": body.get("id")})

    user_id = int(body.get("id"))
    role = "volunteer"
    name = (body.get("name") or "").strip()
    city = (body.get("city") or "").strip()
    phone = (body.get("phone") or "").strip()
    gender = body.get("gender")

    try:
        age = int(body.get("age"))
    except (TypeError, ValueError):
        age = None

    if not name or not city or not phone or gender not in {"f", "m"} or age is None:
        response_body = {"error": "invalid_profile_data"}
    else:
        try:
            async with async_session() as db:
                result = await db.execute(
                    select(User).where(User.telegram_id == user_id)
                )
                user = result.scalar_one_or_none()

                if user is None:
                    user = User(
                        telegram_id=user_id,
                        role=role,
                        name=name,
                        age=age,
                        gender=gender,
                        city=city,
                        phone=phone,
                        profile_filled=True,
                    )
                    db.add(user)
                else:
                    user.role = role
                    user.name = name
                    user.age = age
                    user.gender = gender
                    user.city = city
                    user.phone = phone
                    user.profile_filled = True

                await db.commit()
                logger.info("БД СДЕЛАЛО ПРОФИЛЬ ВОЛОНТЕРА", extra={"body": body.get("id")})
                response_body = {
                    "id": user.id,
                    "telegram_id": user.telegram_id,
                    "role": user.role,
                    "name": user.name,
                    "age": user.age,
                    "city": user.city,
                    "phone": user.phone,
                    "gender": gender,
                }
        except SQLAlchemyError:
            logger.exception("ОШИБКА ПРИ СОЗДАНИИ ПРОФИЛЯ ВОЛОНТЕРА", extra={"body": body.get("id")})
            response_body = {"error": "db_error"}

            
    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", ExchangeType.TOPIC, durable=True
        )
        user_queue = await channel.declare_queue(
            settings.USER_QUEUE.format(user_id=user_id), durable=True
        )
        await user_queue.bind(
            exchange,
            settings.USER_QUEUE.format(user_id=user_id),
        )
        await exchange.publish(
            aio_pika.Message(msgpack.packb(response_body)),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )

    