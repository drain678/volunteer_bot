from prometheus_client import Counter, Histogram

RECEIVE_MESSAGE = Counter("receive_message_from_queue", "Принятые сообщения из очереди")

PROCESSED_MESSAGES = Counter(
    "consumer_messages_processed_total",
    "Количество обработанных сообщений consumer по action/status",
    ["action", "status"],
)

MESSAGE_PROCESSING_SECONDS = Histogram(
    "consumer_message_processing_seconds",
    "Время обработки сообщения consumer по action",
    ["action"],
)
