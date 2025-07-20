from typing import TYPE_CHECKING
from ..resources import *
from ..utils import RuntimeHelper

if TYPE_CHECKING:
    from ..client import PowerBIClient


class DMV:
    """
    A convenience wrapper for exporting Power BI resources
    (Groups, Datasets, Reports, etc.) as Spark DataFrames.
    """

    def __init__(self, client: "PowerBIClient") -> None:
        self.client = client

    @RuntimeHelper.to_sparkdf
    async def groups(self):
        """
        Returns all accessible workspaces as a Spark DataFrame.
        """
        async with self.client as session:
            groups = await session.list_groups()
            return self.client.to_df(groups)

    @RuntimeHelper.to_sparkdf
    async def datasets(self):
        """
        Returns all datasets across all workspaces as a Spark DataFrame.
        """
        async with self.client as session:
            groups = await session.list_groups()
            datasets = await session.list_datasets(*groups)
            return self.client.to_df(datasets)

    @RuntimeHelper.to_sparkdf
    async def dataflows(self):
        """
        Returns all dataflows across all workspaces as a Spark DataFrame.
        """
        async with self.client as session:
            groups = await session.list_groups()
            dataflows = await session.list_dataflows(*groups)
            return self.client.to_df(dataflows)

    @RuntimeHelper.to_sparkdf
    async def reports(self):
        """
        Returns all reports across all workspaces as a Spark DataFrame.
        """
        async with self.client as session:
            groups = await session.list_groups()
            reports = await session.list_reports(*groups)
            return self.client.to_df(reports)

    @RuntimeHelper.to_sparkdf
    async def pages(self):
        """
        Returns all report pages (tabs) across all reports in all workspaces.
        """
        async with self.client as session:
            groups = await session.list_groups()
            reports = await session.list_reports(*groups)
            pages = await session.list_pages(*reports)
            return self.client.to_df(pages)

    @RuntimeHelper.to_sparkdf
    async def schedules(self):
        """
        Returns all dataset refresh schedules across workspaces.
        """
        async with self.client as session:
            groups = await session.list_groups()
            datasets = await session.list_datasets(*groups)
            schedules = await session.list_schedules(*datasets)
            return self.client.to_df(schedules)

    @RuntimeHelper.to_sparkdf
    async def dataset_refresh_history(self):
        """
        Returns refresh history records (datasets).
        """
        async with self.client as session:
            groups = await session.list_groups()
            datasets = await session.list_datasets(*groups)
            history = await session.list_refresh_history(*datasets)
            return self.client.to_df(list(history))

    @RuntimeHelper.to_sparkdf
    async def dataflow_refresh_history(self):
        """
        Returns refresh history records (dataflows).
        """
        async with self.client as session:
            groups = await session.list_groups()
            dataflows = await session.list_dataflows(*groups)
            history = await session.list_refresh_history(*dataflows)
            return self.client.to_df(list(history))
