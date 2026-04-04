import logging
from pathlib import Path
from contextvars import ContextVar


# Context variable to store run_id
current_run_id: ContextVar[str] = ContextVar("run_id", default="SYSTEM")


def set_run_id(run_id: str) -> None:
    current_run_id.set(str(run_id))


def get_run_id() -> str:
    return current_run_id.get()


class RunIdFilter(logging.Filter):
    """
    Injects run_id into every log record
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = get_run_id()
        return True


def get_logger(name: str) -> logging.Logger:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / "pipeline.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | run_id=%(run_id)s | %(message)s"
    )

    # File handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Filter (IMPORTANT)
    run_id_filter = RunIdFilter()

    file_handler.addFilter(run_id_filter)
    console_handler.addFilter(run_id_filter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger