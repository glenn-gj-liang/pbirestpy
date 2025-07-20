from functools import wraps
from typing import Callable, Any
import pandas as pd
import inspect


class RuntimeHelper:

    @staticmethod
    def is_on_databricks() -> bool:
        try:
            import databricks.sdk.runtime  # type: ignore # noqa

            return True
        except ImportError:
            return False

    @staticmethod
    def to_sparkdf(func: Callable[..., Any]) -> Callable[..., Any]:
        """
        Decorator to convert return value of a function (sync or async)
        to Spark DataFrame if running on Databricks.

        - If the function is async, awaits the result.
        - If the result is a list or pandas DataFrame, converts to Spark DataFrame on Databricks.
        """

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            return RuntimeHelper._to_df(result)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            return RuntimeHelper._to_df(result)

        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    @staticmethod
    def _to_df(data: Any) -> Any:
        """
        Convert a list/dict/pandas.DataFrame to Spark DataFrame if on Databricks.
        Otherwise return pandas.DataFrame or original data.
        """
        if RuntimeHelper.is_on_databricks():
            from databricks.sdk.runtime import spark  # type: ignore

            if isinstance(data, pd.DataFrame):
                return spark.createDataFrame(data)
            return spark.createDataFrame(data)
        return data
