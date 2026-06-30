"""Shared utilities: logger + timezone-aware date helpers.

Trimmed from propensity_model2/modules/utils.py — dropped send_email, SFTP CSV
helpers, and date-batching (unused by the pipeline). Console logging only, so
Airflow captures it via stdout.
"""
import logging
import sys
from datetime import datetime, timedelta

import pytz
from pandas import to_datetime

LOG_TIMEZONE      = "Asia/Kolkata"
DEFAULT_TIME_ZONE = "Asia/Calcutta"


class _TZFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, tz=None):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.tz = pytz.timezone(tz or "UTC")

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=self.tz)
        return dt.strftime(datefmt) if datefmt else dt.isoformat()


def setup_logger(name: str = "propensity") -> logging.Logger:
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)  # stdout -> Airflow logs as INFO (not ERROR)
        handler.setFormatter(_TZFormatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S %Z%z",
            tz=LOG_TIMEZONE,
        ))
        log.addHandler(handler)
    return log


logger = setup_logger()


def get_current_datetime(timezone: str = None) -> str:
    """Timestamp string 'YYYYMMDD-HHMMSS' in the given tz (default Asia/Calcutta)."""
    tz = pytz.timezone(timezone or DEFAULT_TIME_ZONE)
    return datetime.now(tz).strftime("%Y%m%d-%H%M%S")


def get_current_date(timezone: str = None, delta: int = 0) -> str:
    """Date string 'YYYY-MM-DD' in the given tz, optionally minus `delta` days."""
    tz = pytz.timezone(timezone or DEFAULT_TIME_ZONE)
    dt = datetime.now(tz)
    if delta:
        dt -= timedelta(days=delta)
    return dt.strftime("%Y-%m-%d")


def get_datetime(timezone: str = None, delta: int = 0,
                 start_of_day: bool = False, end_of_day: bool = False,
                 pandas_datetime: bool = False, replace_timezone: bool = True) -> datetime:
    """Datetime object in the given tz, with optional day-delta / day-boundary snapping."""
    tz = pytz.timezone(timezone or DEFAULT_TIME_ZONE)
    dt = datetime.now(tz)
    if delta:
        dt -= timedelta(days=delta)
    if start_of_day:
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    if replace_timezone:
        dt = dt.replace(tzinfo=None)
    if pandas_datetime:
        dt = to_datetime(dt)
    return dt
