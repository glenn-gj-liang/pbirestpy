from concurrent.futures import ThreadPoolExecutor
from typing import Iterator, TYPE_CHECKING
import re
import asyncio
from pandas import DataFrame, concat
from requests import post
from ..utils import RuntimeHelper

if TYPE_CHECKING:
    from . import Dataset

NORM_PATT = {".*?\[": "", "\]*?": ""}  # type: ignore


class ResponseParser:
    def __init__(self, responses) -> None:
        """
        Initializes the ResponseParser with a list of responses.

        Args:
            responses (List): A list of responses to parse.
        """
        self.responses = responses

    @staticmethod
    def _rename_column(
        column_name: str,
        rename_patterns: dict = NORM_PATT,
    ) -> str:
        """
        Renames a column based on the provided patterns.

        Args:
            column_name (str): The original column name.
            rename_patterns (dict): A dictionary of patterns to rename the column.

        Returns:
            str: The renamed column name.
        """
        for pattern, replacement in rename_patterns.items():
            column_name = re.sub(pattern, replacement, column_name)
        return column_name

    @staticmethod
    def _construct_dataframe(response) -> DataFrame:
        """
        Constructs a DataFrame from the provided data.

        Args:
            data (List[dict]): The data to construct the DataFrame from.
            rename_patterns (dict): A dictionary of patterns to rename the columns.

        Returns:
            DataFrame: The constructed DataFrame.
        """
        if "results" not in response:
            return DataFrame()
        df = DataFrame(response["results"][0]["tables"][0]["rows"])
        df.columns = [ResponseParser._rename_column(col) for col in df.columns]
        return df

    @staticmethod
    def _gather_dataframes(
        results: Iterator[DataFrame],
    ) -> DataFrame:
        filtered_dfs = [df for df in results if not df.shape[0] == 0]
        result = DataFrame()
        if filtered_dfs:
            result = concat(filtered_dfs, ignore_index=True)
        return result

    @RuntimeHelper.to_sparkdf
    def parse(self) -> DataFrame:
        """
        Parses the responses and constructs a DataFrame.

        Returns:
            DataFrame: The parsed DataFrame.
        """

        return self._gather_dataframes(
            self._construct_dataframe(response) for response in self.responses
        )


class DaxExecutor:
    def __init__(self, dataset: "Dataset") -> None:
        """
        Initializes the DaxExecutor with a dataset.

        Args:
            dataset (Dataset): The dataset to execute DAX queries on.
        """
        self.dataset = dataset

    @staticmethod
    def _build_payload(query: str) -> dict:
        """
        Builds the payload for the DAX query.

        Args:
            query (str): The DAX query to execute.

        Returns:
            dict: The payload for the DAX query.
        """
        return {"queries": [{"query": query}]}

    def submit_query(self, query: str) -> dict:
        """
        Submits a DAX query and returns the response.

        Args:
            query (str): The DAX query to execute.

        Returns:
            dict: The response from the DAX query.
        """
        payload = self._build_payload(query)
        headers = self.dataset.client.session._build_headers({})
        response = post(
            self.dataset.dax_query_url, json=payload, headers=headers
        )
        return response.json()

    async def async_submit_query(self, query: str) -> dict:
        """
        Asynchronously submits a DAX query and returns a ResponseParser.

        Args:
            query (str): The DAX query to execute.

        Returns:
            ResponseParser: The parser for the response.
        """
        payload = self._build_payload(query)
        response = await self.dataset.client.session.post(
            self.dataset.dax_query_url, json=payload
        )
        return await response.json()

    def execute(self, *queries):
        with ThreadPoolExecutor() as executor:
            futures = executor.map(self.submit_query, queries)
            return ResponseParser(responses=futures).parse()

    async def async_execute(self, *queries):
        tasks = [
            asyncio.create_task(self.async_submit_query(query))
            for query in queries
        ]
        responses = await asyncio.gather(*tasks)
        return ResponseParser(responses=responses).parse()
