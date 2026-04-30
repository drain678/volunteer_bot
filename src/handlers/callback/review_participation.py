import asyncio
import logging.config
import aio_pika
import msgpack
from aio_pika import ExchangeType
from aio_pika.exceptions import QueueEmpty
from aiogram.exceptions import TelegramBadRequest
from consumer.logger import LOGGING_CONFIG, logger
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config.settings import settings
from src.handlers.callback.router import router
from src.handlers.state.participation_review import ParticipationReviewState
from src.storage.rabbit import channel_pool


async def _request_to_consumer(
    user_id: int, action: str, payload: dict | None = None
) -> dict | None:
    logging.config.dictConfig(LOGGING_CONFIG)
    body = {"id": user_id, "action": action}

    if payload:
        body.update(payload)

    async with channel_pool.acquire() as channel:
        exchange = await channel.declare_exchange("user_form", ExchangeType.TOPIC, durable=True)
        queue = await channel.declare_queue("user_messages", durable=True)
        user_queue = await channel.declare_queue(
            settings.USER_QUEUE.format(user_id=user_id), durable=True
        )
        await queue.bind(exchange, "user_messages")
        await user_queue.bind(exchange, settings.USER_QUEUE.format(user_id=user_id))

        await exchange.publish(
            aio_pika.Message(msgpack.packb(body)),
            routing_key="user_messages",
        )
        logger.info("ОТПРАВИЛИ ЗАПРОС НА ПРИНЯТИЕ ЗАЯВКИ", extra={"body": body})
        for _ in range(10):
            try:
                res = await user_queue.get(timeout=3)
                await res.ack()
                return msgpack.unpackb(res.body)
            except QueueEmpty:
                await asyncio.sleep(1)
    return None


@router.callback_query(lambda c: c.data.startswith("participation_approve_"))
async def approve_participation(callback: CallbackQuery) -> None:
    try:
        participation_id = int(callback.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректная заявка", show_alert=True)
        return

    response = await _request_to_consumer(
        callback.from_user.id,
        "review_participation",
        {"participation_id": participation_id, "decision": "approve"},
    )
    if not response or "error" in response:
        await callback.answer("Не удалось принять заявку", show_alert=True)
        return

    volunteer_tg = response.get("volunteer_telegram_id")
    event_title = response.get("event_title")
    if volunteer_tg:
        try:
            await callback.bot.send_message(
                volunteer_tg,
                f"Ваша заявка на мероприятие «{event_title}» принята.",
            )
        except TelegramBadRequest:
            pass
    await callback.answer("Заявка принята", show_alert=True)


@router.callback_query(lambda c: c.data.startswith("participation_reject_"))
async def reject_participation_start(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        participation_id = int(callback.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректная заявка", show_alert=True)
        return

    await state.set_state(ParticipationReviewState.reject_reason_input)
    await state.update_data(reject_participation_id=participation_id)
    await callback.message.answer("Напишите причину отклонения заявки:")
    await callback.answer()


@router.message(ParticipationReviewState.reject_reason_input)
async def reject_participation_reason(message: Message, state: FSMContext) -> None:
    reason = (message.text or "").strip()
    if not reason:
        await message.answer("Причина не должна быть пустой.")
        return

    data = await state.get_data()
    participation_id = data.get("reject_participation_id")
    if not participation_id:
        await message.answer("Не удалось найти заявку.")
        await state.clear()
        return

    response = await _request_to_consumer(
        message.from_user.id,
        "review_participation",
        {
            "participation_id": participation_id,
            "decision": "reject",
            "reason": reason,
        },
    )
    if not response or "error" in response:
        await message.answer("Не удалось отклонить заявку.")
        await state.clear()
        return

    volunteer_tg = response.get("volunteer_telegram_id")
    event_title = response.get("event_title") or "без названия"
    if reason:
        reject_text = (
            "\n\nБлагодарим за проявленный интерес к мероприятию "
            f"«{event_title}», но ваша заявка была отклонена по причине: {reason}"
        )
    if volunteer_tg:
        try:
            await message.bot.send_message(
                volunteer_tg,
                reject_text,
            )
        except TelegramBadRequest:
            pass
    await message.answer("Заявка отклонена.")
    await state.clear()
