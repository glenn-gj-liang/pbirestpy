from functools import lru_cache
from os import environ
from typing import Callable


class RuntimeHelper:
    @staticmethod
    @lru_cache(maxsize=None)
    def is_on_databricks():
        """
        Checks if the code is running on Databricks by looking for the
        DATABRICKS_RUNTIME_VERSION environment variable.
        Returns:
            bool: True if running on Databricks, False otherwise.
        """

        if environ.get("DATABRICKS_RUNTIME_VERSION"):
            return True
        return False

    @staticmethod
    def to_sparkdf(func: Callable):
        """
        Decorator to convert a function's return value to a Spark DataFrame.
        This is a placeholder for actual implementation.
        """

        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if RuntimeHelper.is_on_databricks():
                # Convert result to Spark DataFrame if running on Databricks
                from databricks.sdk.runtime import spark  # type: ignore

                result = spark.createDataFrame(result)
            return result

        return wrapper
