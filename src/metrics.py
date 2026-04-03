import time
from functools import wraps
from typing import Any, Callable

from prometheus_client import Counter, Histogram

BUCKETS = [
    0.2,
    0.4,
    0.6,
    0.8,
    1.0,
    1.2,
    1.4,
    1.6,
    1.8,
    2.0,
    float("+inf"),
]

LATENCY = Histogram(
    "latency_seconds_handler",
    "считает задержку",
    labelnames=["handler"],
    buckets=BUCKETS,
)


def track_latency(method_name: str):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.monotonic()
            try:
                return await func(*args, **kwargs)
            finally:
                end_time = time.monotonic() - start_time
                LATENCY.labels(handler=method_name).observe(end_time)

        return wrapper

    return decorator


NEW_PROFILES = Counter("new_profile_totoal", "считает количество созданных анкет")
SEND_MESSAGE = Counter(
    "bot_messages_sent",
    "Отправленные сообщения в очередь",
)
