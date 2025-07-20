from aiohttp import ClientResponseError
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_random,
    before_sleep_log,
)
from logging import WARNING
from ..utils import Logger

# Custom logger instance for retry-related logs
logger = Logger(name="Tenacity.Retry")

# Default fallback wait time (in seconds) if Retry-After header is not present
DEFAULT_RETRY_AFTER = 60


class Retry:
    """
    Retry policy utilities for handling common HTTP error scenarios with Power BI API.

    Includes decorators for:
    - Handling 400/409 conflict errors with randomized backoff
    - Handling 429 rate limiting errors with respect to the Retry-After header
    """

    @staticmethod
    def is_conflict_error(exception: BaseException) -> bool:
        """
        Check whether the exception is a 400 (Bad Request) or 409 (Conflict) error
        typically returned by Power BI when multiple refreshes or bad input occurs.
        """
        return isinstance(
            exception, ClientResponseError
        ) and exception.status in (409, 400)

    @staticmethod
    def on_conflict():
        """
        Retry decorator for 400 and 409 errors.

        Retries up to 5 times with randomized wait (10 to 20 seconds) between attempts.
        Logs each retry attempt using the custom logger.
        """
        return retry(
            retry=retry_if_exception(Retry.is_conflict_error),
            stop=stop_after_attempt(5),
            wait=wait_random(min=10, max=20),
            before_sleep=before_sleep_log(logger, log_level=WARNING),
        )

    @staticmethod
    def is_rate_limit_error(exception: BaseException) -> bool:
        """
        Check whether the exception is a 429 (Too Many Requests) error
        returned when API rate limits are exceeded.
        """
        return (
            isinstance(exception, ClientResponseError)
            and exception.status == 429
        )

    @staticmethod
    def get_retry_after(exception: ClientResponseError) -> float | None:
        """
        Attempt to parse the Retry-After header from the exception.
        Returns the number of seconds to wait, or None if header is absent or invalid.
        """
        retry_after = exception.headers.get("Retry-After")  # type: ignore
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                return None
        return None

    @staticmethod
    def log_retry_after(retry_state):
        """
        Custom logging hook for retries:
        - Logs Retry-After value for 429 errors
        - Logs generic info for other exceptions
        """
        exc = retry_state.outcome.exception()
        if isinstance(exc, ClientResponseError) and exc.status == 429:
            retry_after = Retry.get_retry_after(exc)
            if retry_after:
                logger.warning(
                    f"[RateLimit] 429 Too Many Requests. "
                    f"Retrying in {retry_after} seconds (from Retry-After header)."
                )
            else:
                logger.warning(
                    f"[RateLimit] 429 Too Many Requests. "
                    f"Retrying in default wait time."
                )
        else:
            logger.warning(
                f"[Retry] {type(exc).__name__}: {exc} | "
                f"Attempt #{retry_state.attempt_number}, "
                f"Next wait: {retry_state.next_action.sleep:.1f}s"
            )

    @staticmethod
    def on_rate_limit():
        """
        Retry decorator for 429 Too Many Requests errors.

        Attempts up to 10 retries, honoring Retry-After header if present.
        Falls back to DEFAULT_RETRY_AFTER seconds if header is missing or invalid.
        """

        def custom_wait(retry_state):
            exc = retry_state.outcome.exception()
            if isinstance(exc, ClientResponseError):
                retry_after = Retry.get_retry_after(exc)
                if retry_after:
                    return retry_after
            return DEFAULT_RETRY_AFTER

        return retry(
            retry=retry_if_exception(Retry.is_rate_limit_error),
            stop=stop_after_attempt(10),
            wait=custom_wait,
            before_sleep=Retry.log_retry_after,
        )
