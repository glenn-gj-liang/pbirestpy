from datetime import datetime
from typing import Optional
import pytz
from dateutil.parser import isoparse

DEFAULT_TIMEZONE = "Asia/Shanghai"
DEFAULT_PARSED_DATETIME = isoparse("2099-01-01T00:00:00Z")


class DatetimeHelper:
    """A utility class for handling datetime operations."""

    @staticmethod
    def get_current_datetime(timezone: str = DEFAULT_TIMEZONE) -> datetime:
        """
        Returns the current datetime in the specified timezone.
        Args:
            timezone (str): The name of the timezone to use. Defaults to "Asia/Shanghai".
        Raises:
            ValueError: If the provided timezone is invalid.
        """
        if timezone not in pytz.all_timezones:
            raise ValueError(f"Invalid timezone: {timezone}")
        tz = pytz.timezone(timezone)
        return datetime.now(tz)

    @staticmethod
    def parse_datetime(
        date_str: Optional[str] = None, timezone: str = DEFAULT_TIMEZONE
    ) -> datetime:
        """
        Parses a date string into a datetime object in the specified timezone.
        If the date string is None or empty, returns a default datetime.
        Args:
            date_str (Optional[str]): The date string to parse.
            timezone (str): The name of the timezone to use. Defaults to "UTC".
        """
        if timezone not in pytz.all_timezones:
            raise ValueError(f"Invalid timezone: {timezone}")
        if date_str is None or date_str == "":
            return DEFAULT_PARSED_DATETIME
        parsed = isoparse(date_str)
        tz = pytz.timezone(timezone)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=pytz.utc)
        return parsed.astimezone(tz)
