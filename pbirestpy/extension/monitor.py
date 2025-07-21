from datetime import timedelta
from json import dumps
from typing import Callable, TYPE_CHECKING
from pandas import DataFrame, merge
import asyncio
from logging import getLogger, Formatter, StreamHandler

from ..utils import DatetimeHelper

if TYPE_CHECKING:
    from ..client import PowerBIClient
    from ..resources import *


class AdaptiveCard:
    """
    Generate Adaptive Card payloads for displaying Power BI refresh statuses.
    """

    def __init__(self, title: str, df: DataFrame):
        self.title = title
        self.df = df

    @staticmethod
    def _build_title(title: str) -> dict:
        """
        Build the title section of the card, including current timestamp.
        """
        cur_time = DatetimeHelper.get_current_datetime().strftime(
            "%Y-%m-%d %H:%M"
        )
        return {
            "type": "Container",
            "items": [
                {
                    "type": "TextBlock",
                    "text": title,
                    "wrap": True,
                    "style": "heading",
                    "separator": True,
                    "size": "Large",
                    "fontType": "Monospace",
                },
                {
                    "type": "TextBlock",
                    "text": cur_time,
                    "wrap": True,
                    "separator": True,
                    "size": "Small",
                    "isSubtle": True,
                    "color": "Accent",
                    "fontType": "Monospace",
                },
            ],
        }

    @staticmethod
    def _build_table_cell(content: str, style: str, min_hieght: str = "60px"):
        """
        Build a single table cell for the Adaptive Card table.
        """
        return {
            "type": "TableCell",
            "items": [
                {
                    "type": "TextBlock",
                    "text": content,
                    "wrap": True,
                    "height": "stretch",
                    "horizontalAlignment": "Left",
                    "fontType": "Monospace",
                }
            ],
            "horizontalAlignment": "Center",
            "minHeight": min_hieght,
            "style": style,
        }

    @staticmethod
    def _build_table_rows(
        data: DataFrame, status: RefreshStatus, style: str = "default"
    ):
        """
        Build table rows for a given refresh status.
        """
        df = data.loc[data["Status"] == str(status), :].sort_values(
            by="EndTime", ascending=False
        )
        rows = []
        for _, row_data in df.iterrows():
            row = {
                "type": "TableRow",
                "cells": [
                    AdaptiveCard._build_table_cell(
                        row_data["Dashboard"], style
                    ),
                    AdaptiveCard._build_table_cell(row_data["EndTime"], style),
                ],
            }
            rows.append(row)
        return rows

    @staticmethod
    def _build_table(data: DataFrame):
        """
        Build the complete table layout for the Adaptive Card.
        """
        header_row = {
            "type": "TableRow",
            "cells": [
                AdaptiveCard._build_table_cell("Dashboard", "Default", "30px"),
                AdaptiveCard._build_table_cell("EndTime", "Default", "30px"),
            ],
            "verticalCellContentAlignment": "Center",
        }
        build_rows = AdaptiveCard._build_table_rows
        return {
            "type": "Table",
            "columns": [{"width": 5}, {"width": 2}],
            "rows": [
                header_row,
                *build_rows(data, RefreshStatus.Failed, "Attention"),
                *build_rows(data, RefreshStatus.Cancelled, "Attention"),
                *build_rows(data, RefreshStatus.Completed, "Good"),
                *build_rows(data, RefreshStatus.InProgress, "Accent"),
                *build_rows(data, RefreshStatus.Pending, "Default"),
            ],
        }

    @staticmethod
    def _build_kpi_column(data: DataFrame, status: RefreshStatus):
        """
        Build a KPI column showing the count for a given refresh status.
        """
        color = {
            RefreshStatus.Pending: "Dark",
            RefreshStatus.InProgress: "Accent",
            RefreshStatus.Completed: "Good",
            RefreshStatus.Failed: "Attention",
            RefreshStatus.Cancelled: "Attention",
        }
        df = data.loc[data["Status"] == str(status), :].sort_values(
            by="EndTime", ascending=False
        )
        value = df.count()
        return {
            "type": "Column",
            "items": [
                {
                    "type": "TextBlock",
                    "text": f"**{value}**",
                    "wrap": True,
                    "color": color.get(status, "Default"),
                    "horizontalAlignment": "Center",
                },
                {
                    "type": "TextBlock",
                    "spacing": "None",
                    "text": str(status),
                    "wrap": True,
                    "fontType": "Monospace",
                    "horizontalAlignment": "Center",
                },
            ],
            "width": 1,
        }

    @staticmethod
    def _build_kpi(data: DataFrame):
        """
        Build the full KPI summary section.
        """
        build_col = AdaptiveCard._build_kpi_column
        return {
            "type": "Container",
            "items": [
                {
                    "type": "ColumnSet",
                    "horizontalAlignment": "Center",
                    "columns": [
                        build_col(data, RefreshStatus.Pending),
                        build_col(data, RefreshStatus.InProgress),
                        build_col(data, RefreshStatus.Failed),
                    ],
                    "separator": True,
                    "spacing": "Large",
                },
                {
                    "type": "ColumnSet",
                    "horizontalAlignment": "Center",
                    "columns": [
                        build_col(data, RefreshStatus.Completed),
                        build_col(data, RefreshStatus.Total),
                    ],
                    "separator": True,
                    "spacing": "Large",
                },
            ],
        }

    def __str__(self):
        """
        Convert the AdaptiveCard instance to a JSON string representation.
        """
        return dumps(
            {
                "type": "AdaptiveCard",
                "body": [
                    self._build_title(self.title),
                    self._build_kpi(self.df),
                    self._build_table(self.df),
                ],
                "$schema": "https://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.5",
            }
        )


class CacheData:
    """
    Manage and calculate refresh cache information for datasets.
    """

    def __init__(self, df: DataFrame, aliases: dict):
        # Filter out refreshes triggered via XMLA endpoint
        self.df = df.loc[df["refreshType"] != "ViaXmlaEndpoint", :].copy()
        self.aliases = aliases

    @property
    def latest_refreshes(self):
        """
        Get the latest refresh (of any status) for each dataset.
        """
        df = self.df
        df = df.loc[df["refreshType"] != "ViaXmlaEndpoint", :]
        latest_df = (
            df.sort_values("startTime")
            .groupby("dataset_name", as_index=False)
            .tail(1)
        )
        latest_df = latest_df[["dataset_name", "startTime", "status"]].rename(
            columns={
                "startTime": "last_refresh_time",
                "status": "last_refresh_status",
            }
        )
        return latest_df

    @property
    def latest_completed_refreshes(self):
        """
        Get the latest *completed* refresh for each dataset.
        """
        df = self.df
        completed_df = df[df["status"].astype(str) == "COMPLETED"]
        latest_completed_df = (
            completed_df.sort_values("startTime")
            .groupby("dataset_name", as_index=False)
            .tail(1)
        )
        latest_completed_df = latest_completed_df[
            ["dataset_name", "startTime"]
        ].rename(columns={"startTime": "last_completed_time"})
        return latest_completed_df

    @staticmethod
    def _calculate_status(row_data):
        """
        Placeholder for custom logic to determine overall status.
        """
        return row_data

    @staticmethod
    def _calculate_content(row_data):
        """
        Placeholder for custom logic to determine display content.
        """
        return row_data

    @property
    def calculated(self):
        """
        Merge and process the refresh information into final output format.
        """
        new_cache = merge(
            self.latest_refreshes,
            self.latest_completed_refreshes,
            on="dataset_name",
            how="outer",
        )

        new_cache = new_cache[
            [
                "dataset_name",
                "last_completed_time",
                "last_refresh_time",
                "last_refresh_status",
            ]
        ]
        new_cache["Dashboard"] = new_cache["dataset_name"]
        if isinstance(self.aliases, dict):
            new_cache["Dashboard"] = new_cache["dataset_name"].map(
                self.aliases
            )

        new_cache["Staus"] = new_cache.apply(self._calculate_status, axis=1)
        new_cache["EndTime"] = new_cache.apply(self._calculate_content, axis=1)

        return new_cache.sort_values(by=list(new_cache.columns)).reset_index(
            drop=True
        )

    def __eq__(self, other: object) -> bool:
        """
        Check if the calculated cache is equal to another.
        """
        if other is None:
            return False
        return self.calculated.equals(other.calculated)  # type: ignore


class MonitorSetting:
    """
    Manage monitoring configuration for a Power BI dataset group.
    """

    def __init__(
        self,
        client: "PowerBIClient",
        name: str,
        group_name: str,
        datasets,
        on_change: Callable,
        timeout: int = 3600,
        interval: int = 90,
    ):
        self.name = name
        self.start_time = DatetimeHelper.get_current_datetime()
        self.stop_time = self.start_time + timedelta(seconds=timeout)
        self.client = client
        self.on_change = on_change
        self.group_name = group_name
        self.datasets = datasets
        self.interval = interval
        self.logger = self.get_logger(name)
        self.__cache: CacheData = None  # type: ignore

    @staticmethod
    def get_logger(name: str):
        """
        Create a logger instance for this monitor.
        """
        formatter = Formatter(
            fmt="[%(asctime)s][%(name)s][%(levelname)s] %(message)s"
        )
        hander = StreamHandler()
        hander.setFormatter(formatter)
        logger = getLogger(name)
        logger.addHandler(hander)
        return logger

    @property
    def cache_data(self):
        return self.__cache

    @cache_data.setter
    def cache_data(self, new_cache: CacheData):
        """
        Update the cache and trigger change event if data has changed.
        """
        if self.__cache is None or self.__cache != new_cache:
            self.__cache = new_cache
            self.on_change(self)
            self.logger.info("Status changed.")

    async def fetch_data(self):
        """
        Fetch the latest dataset refreshes from Power BI API.
        """
        async with self.client as session:
            groups = await session.list_groups()
            group = next(g for g in groups if g.name == self.group_name)
            datasets = filter(
                lambda x: x.name in self.datasets,
                await session.list_datasets(group),
            )
            refreshes = await session.list_refresh_history(*datasets)
            self.cache_data = CacheData(
                df=self.client.to_df(refreshes), aliases=self.datasets
            )

    @property
    def adaptive_card(self):
        """
        Return the current status as an Adaptive Card JSON string.
        """
        return str(AdaptiveCard(title=self.name, df=self.__cache.calculated))

    @property
    def running(self):
        """
        Check if monitor is still within its allowed execution window.
        """
        cur_dt = DatetimeHelper.get_current_datetime()
        return self.start_time < cur_dt <= self.stop_time

    async def __call__(self):
        """
        Main loop that fetches refresh data periodically.
        """
        self.logger.info(f"registered.")
        while self.running:
            await self.fetch_data()
            await asyncio.sleep(self.interval)
        self.logger.info(f"exited.")


async def monitor(*settings):
    """
    Run multiple MonitorSetting instances concurrently.
    """
    tasks = [asyncio.create_task(setting()) for setting in settings]
    await asyncio.gather(*tasks)
