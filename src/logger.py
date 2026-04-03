import logging.config
from contextvars import ContextVar
from pathlib import Path
from uuid import uuid4

import yaml
from colorama import Fore, Style

correlation_id_context: ContextVar[str] = ContextVar("correlation_id", default="N/A")


def setup_logging():
    config_path = Path(__file__).resolve().parent.parent / "config" / "logging.conf.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    logging.config.dictConfig(config)


def get_logger(name: str = "backend_logger"):
    return logging.getLogger(name)


# with open("config/logging.conf.yml", "r") as f:
#     LOGGING_CONFIG = yaml.full_load(f)


class ConsoleFormatter(logging.Formatter):
    LEVEL_COLORS = {
        "DEBUG": Fore.BLUE,
        "INFO": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "CRITICAL": Fore.MAGENTA + Style.BRIGHT,
    }

    def format(self, record: logging.LogRecord) -> str:
        record.correlation_id = correlation_id_context.get()
        log_color = self.LEVEL_COLORS.get(record.levelname, "")
        reset = Style.RESET_ALL
        record.levelname = f"{log_color}{record.levelname}{reset}"

        return super().format(record)
    

# def setup_logging():
#     config_path = Path(__file__).resolve().parent.parent / "config" / "logging.conf.yml"

#     with open(config_path) as f:
#         config = yaml.safe_load(f)

#     logging.config.dictConfig(config)


# def get_logger(name: str = "backend_logger") -> logging.Logger:
#     return logging.getLogger(name)


# def set_correlation_id() -> str:
#     correlation_id = str(uuid4())
#     correlation_id_context.set(correlation_id)
#     return correlation_id


# logger = logging.getLogger("backend_logger")
