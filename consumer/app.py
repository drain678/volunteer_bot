import logging.config
import asyncio

import aio_pika
import msgpack
from aiormq.exceptions import AMQPConnectionError
from sqlalchemy import select

from config.settings import settings
from consumer.handlers.event_distribution import handle_event_distribution
from consumer.logger import LOGGING_CONFIG, logger
from consumer.metrics import RECEIVE_MESSAGE
from consumer.storage import rabbit
from consumer.storage.db import async_session
from src.models.models import Organization, User

ORGANIZER_RESTRICTED_ACTIONS = {
    "create_event",
    "get_my_events",
    "get_event_participants",
    "review_participation",
    "delete_event",
    "update_event",
    "update_organization",
    "get_organization",
    "delete_organization",
}


async def _publish_guard_error(user_id: int, error: str) -> None:
    async with rabbit.channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange(
            "user_form", aio_pika.ExchangeType.TOPIC, durable=True
        )
        await exchange.publish(
            aio_pika.Message(msgpack.packb({"error": error})),
            routing_key=settings.USER_QUEUE.format(user_id=user_id),
        )


async def _is_banned_organizer_action(body: dict) -> bool:
    action = body.get("action")
    user_id = body.get("id")
    if action not in ORGANIZER_RESTRICTED_ACTIONS or not user_id:
        return False
    try:
        tg_id = int(user_id)
    except (TypeError, ValueError):
        return False

    async with async_session() as db:
        user_result = await db.execute(select(User).where(User.telegram_id == tg_id))
        user = user_result.scalar_one_or_none()
        if not user or user.role != "organizer":
            return False
        org_result = await db.execute(select(Organization).where(Organization.created_by == user.id))
        organization = org_result.scalar_one_or_none()
        return bool(organization and organization.is_banned)


async def main() -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("Запуск consumer...")
    queue_name = "user_messages"
    while True:
        try:
            async with rabbit.channel_pool.acquire() as channel:
                await channel.set_qos(
                    prefetch_count=10,
                )

                queue = await channel.declare_queue(queue_name, durable=True)
                logger.info("Consumer подписан на очередь user_messages")

                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            RECEIVE_MESSAGE.inc()
                            body = msgpack.unpackb(message.body)
                            if await _is_banned_organizer_action(body):
                                user_id = body.get("id")
                                if user_id is not None:
                                    await _publish_guard_error(int(user_id), "organization_banned")
                                continue
                            await handle_event_distribution(body)
        except AMQPConnectionError as exc:
            logger.warning("RabbitMQ недоступен (%s), повтор через 3 сек", exc)
            await asyncio.sleep(3)
        except Exception:
            logger.exception("Ошибка consumer loop, переподключение через 3 сек")
            await asyncio.sleep(3)
