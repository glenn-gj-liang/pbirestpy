from datetime import timedelta
from json import dumps
from typing import Callable
from pandas import DataFrame, merge
import asyncio
from logging import getLogger, Formatter, StreamHandler
from ..resources import *
from ..utils import DatetimeHelper
from ..client import PowerBIClient


class AdaptiveCard:
    def __init__(self, title: str, df: DataFrame):
        self.title = title
        self.df = df

    @staticmethod
    def _build_title(title: str) -> dict:
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
                *build_rows(data, RefreshStatus.FAILED, "Attention"),
                *build_rows(data, RefreshStatus.COMPLETED, "Good"),
                *build_rows(data, RefreshStatus.IN_PROGRESS, "Accent"),
                *build_rows(data, RefreshStatus.PENDING, "Default"),
            ],
        }

    @staticmethod
    def _build_kpi_column(data: DataFrame, status: RefreshStatus):
        color = {
            RefreshStatus.PENDING: "Dark",
            RefreshStatus.IN_PROGRESS: "Accent",
            RefreshStatus.COMPLETED: "Good",
            RefreshStatus.FAILED: "Attention",
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
                    "color": color,
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
        build_col = AdaptiveCard._build_kpi_column
        return {
            "type": "Container",
            "items": [
                {
                    "type": "ColumnSet",
                    "horizontalAlignment": "Center",
                    "columns": [
                        build_col(data, RefreshStatus.PENDING),
                        build_col(data, RefreshStatus.IN_PROGRESS),
                        build_col(data, RefreshStatus.FAILED),
                    ],
                    "separator": True,
                    "spacing": "Large",
                },
                {
                    "type": "ColumnSet",
                    "horizontalAlignment": "Center",
                    "columns": [
                        build_col(data, RefreshStatus.COMPLETED),
                        build_col(data, RefreshStatus.TOTAL),
                    ],
                    "separator": True,
                    "spacing": "Large",
                },
            ],
        }

    def __str__(self):
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
    def __init__(self, df: DataFrame, aliases: dict):
        self.df = df.loc[df["refreshType"] != "ViaXmlaEndpoint", :].copy()
        self.aliases = aliases

    @property
    def latest_refreshes(self):
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
        return row_data

    @staticmethod
    def _calculate_content(row_data):
        return row_data

    @property
    def calculated(self):
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
        if other is None:
            return False
        return self.calculated.equals(other.calculated)  # type: ignore


class MonitorSetting:
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
        if self.__cache is None or self.__cache != new_cache:
            self.__cache = new_cache
            self.on_change(self)
            self.logger.info("Status changed.")

    async def fetch_data(self):
        async with self.client as session:
            groups = await session.list_groups()
            group = next(g for g in groups if g.name == self.group_name)
            datasets = filter(
                lambda x: x.name in self.datasets,
                await session.list_datasets(group),
            )
            refreshes = await session.list_refreshes(*datasets)
            self.cache_data = CacheData(
                df=self.client.to_df(refreshes), aliases=self.datasets
            )

    @property
    def adaptive_card(self):
        return str(AdaptiveCard(title=self.name, df=self.__cache.calculated))

    @property
    def running(self):
        cur_dt = DatetimeHelper.get_current_datetime()
        return self.start_time < cur_dt <= self.stop_time

    async def __call__(self):
        self.logger.info(f"registered.")
        while self.running:
            await self.fetch_data()
            await asyncio.sleep(self.interval)
        self.logger.info(f"exited.")


async def monitor(*settings):
    tasks = [asyncio.create_task(setting()) for setting in settings]
    await asyncio.gather(*tasks)
