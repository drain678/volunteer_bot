from prometheus_client import Counter

RECEIVE_MESSAGE = Counter("receive_message_from_queue", "Принятые сообщения из очереди")
