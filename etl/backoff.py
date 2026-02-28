import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def backoff(start_sleep_time:float = 0.1, factor:int = 2, border_sleep_time:int = 10):
    """Декоратор для повторных вызовов функции с задержкой"""
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    sleep_time = min(
                        border_sleep_time,
                        start_sleep_time * (factor ** n))
                    logger.error(f"Ошибка в {func.__name__}: {e}. "
                                 f"Повтор через {sleep_time}с.")
                    time.sleep(sleep_time)
                    n += 1
        return inner
    return func_wrapper
