import asyncio
from itertools import chain
from typing import Dict, List, TYPE_CHECKING, Literal
from aiohttp import ClientSession, ClientTimeout

from ..resources import *
from ..utils import Logger, DatetimeHelper
from .retry import Retry

if TYPE_CHECKING:
    from .client import PowerBIClient

# Default request timeout: 10 minutes
DEFAULT_CLIENT_TIMEOUT = ClientTimeout(total=600)

# Delay after canceling a task to ensure backend consistency
POST_CANCEL_DELAY = 5

# Polling interval when checking refresh status
PULL_STATUS_INTERVAL = 10

# Session-level logger
log = Logger(name="ApiSession")


class BaseSession:
    """
    Base class for managing HTTP sessions and performing authenticated requests
    to the Power BI REST API using aiohttp.

    This class supports automatic authentication, retry policies, and
    asynchronous session lifecycle management.
    """

    def __init__(self, client: "PowerBIClient"):
        """
        Initialize the session with a reference to the PowerBIClient.

        Args:
            client (PowerBIClient): The associated Power BI client instance.
        """
        self.client: "PowerBIClient" = client
        self._session: ClientSession = None  # type: ignore # Will be initialized lazily
        self._timeout: ClientTimeout = DEFAULT_CLIENT_TIMEOUT

    def _build_headers(self, kwargs: Dict) -> Dict[str, str]:
        """
        Construct the headers for a request, including Authorization.

        Args:
            kwargs (dict): Optional keyword arguments that may contain headers.

        Returns:
            dict: HTTP headers including Authorization and Content-Type.
        """
        headers = kwargs.get("headers", {})
        authorization = {"Authorization": self.client.authenticator.token}
        headers.update(authorization)

        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

        return headers

    async def init(self):
        """
        Ensure the internal aiohttp session is initialized.

        This can be called explicitly, but is also used internally
        before sending any requests.
        """
        await self._ensure_session()

    async def _ensure_session(self):
        """
        Lazily initialize or reuse an aiohttp.ClientSession with proper timeout.

        Returns:
            ClientSession: An initialized aiohttp session.
        """
        if self._session is None or self._session.closed:
            self._session = ClientSession(timeout=self._timeout)
        return self._session

    async def close(self):
        """
        Gracefully close the aiohttp session if it's still open.
        """
        if self._session and not self._session.closed:
            await self._session.close()

    @Retry.on_rate_limit()
    async def _request(self, method: str, url: str, **kwargs):
        """
        Send an HTTP request with retry handling for rate limits (429).

        This method ensures the session is active, builds the necessary headers,
        and raises an exception for any non-2xx response.

        Args:
            method (str): HTTP method (GET, POST, DELETE, etc.).
            url (str): Fully qualified URL to request.
            **kwargs: Optional arguments passed to aiohttp.request().

        Returns:
            aiohttp.ClientResponse: The response object.
        """
        await self._ensure_session()
        headers = self._build_headers(kwargs)
        kwargs["headers"] = headers
        response = await self._session.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    async def get(self, url: str, **kwargs):
        """
        Send a GET request to the specified URL.

        Args:
            url (str): The target URL.
            **kwargs: Optional arguments passed to the request.

        Returns:
            aiohttp.ClientResponse: The response object.
        """
        return await self._request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs):
        """
        Send a POST request to the specified URL.

        Args:
            url (str): The target URL.
            **kwargs: Optional arguments passed to the request.

        Returns:
            aiohttp.ClientResponse: The response object.
        """
        return await self._request("POST", url, **kwargs)

    async def delete(self, url: str, **kwargs):
        """
        Send a DELETE request to the specified URL.

        Args:
            url (str): The target URL.
            **kwargs: Optional arguments passed to the request.

        Returns:
            aiohttp.ClientResponse: The response object.
        """
        return await self._request("DELETE", url, **kwargs)


class ApiSession(BaseSession):
    """
    High-level session class for interacting with Power BI resources.

    This class builds on BaseSession and provides object-oriented access to
    groups, datasets, reports, dataflows, and related operations such as
    refreshing, scheduling, and retrieving metadata.

    It supports bulk async operations, context-aware refresh control, and
    integrated retry handling for conflict and rate-limit scenarios.
    """

    async def list_groups(self):
        """
        List all workspaces (groups) available to the authenticated user.

        Returns:
            List[Group]: A list of Group model instances.
        """
        response = await self.get(Group.LIST_GROUPS_URL)
        data = (await response.json())["value"]  # type: ignore
        return [Group(**_, client=self.client) for _ in data]

    async def _list_elements_in_groups(self, *groups, attr: str, element_type):
        """
        Internal helper to list elements (datasets, reports, etc.) in multiple groups concurrently.

        Args:
            groups (Group): One or more Group instances.
            attr (str): Attribute name for the URL (e.g., 'datasets').
            element_type (Type): Model class to instantiate from response data.

        Returns:
            List[Any]: A flat list of element instances.
        """
        attr = f"list_{attr}_url"
        tasks = [
            asyncio.create_task(self.get(getattr(group, attr)))
            for group in groups
        ]
        responses = await asyncio.gather(*tasks)
        elements = []
        for group, response in zip(groups, responses):
            data = (await response.json())["value"]  # type: ignore
            in_group = [
                element_type(**_, client=self.client, group=group)
                for _ in data
            ]
            elements.extend(in_group)
        return elements

    async def list_datasets(self, *groups: Group) -> List:
        """
        List all datasets within one or more groups.

        Args:
            groups (Group): One or more Group instances.

        Returns:
            List[Dataset]: A list of Dataset model instances.
        """
        return await self._list_elements_in_groups(
            *groups, attr="datasets", element_type=Dataset
        )

    async def list_dataflows(self, *groups: Group) -> List:
        """
        List all dataflows within one or more groups.

        Args:
            groups (Group): One or more Group instances.

        Returns:
            List[Dataflow]: A list of Dataflow model instances.
        """
        return await self._list_elements_in_groups(
            *groups, attr="dataflows", element_type=Dataflow
        )

    async def list_reports(self, *groups: Group) -> List:
        """
        List all reports within one or more groups.

        Args:
            groups (Group): One or more Group instances.

        Returns:
            List[Report]: A list of Report model instances.
        """
        return await self._list_elements_in_groups(
            *groups, attr="reports", element_type=Report
        )

    async def list_refresh_history(
        self, *refreshables
    ) -> chain[Refresh | Transaction]:
        """
        List refresh history records for one or more datasets or dataflows.

        Args:
            refreshables (Dataset | Dataflow): Refreshable objects to query.

        Returns:
            chain[Refresh | Transaction]: A generator chain of refresh history entries.
        """

        async def fetch_data(refreshable):
            response = await self.get(refreshable.list_refreshes_url)
            data = (await response.json())["value"]
            is_dataset = isinstance(refreshable, Dataset)
            refresh_cls = Refresh if is_dataset else Transaction
            kw_name = "dataset" if is_dataset else "dataflow"
            kwargs = {
                "client": self.client,
                kw_name: refreshable,
                "group": refreshable.group,
            }
            return (refresh_cls(**_, **kwargs) for _ in data)

        tasks = [
            asyncio.create_task(fetch_data(refreshable))
            for refreshable in refreshables
        ]
        return chain(*(await asyncio.gather(*tasks)))

    async def cancel_refresh(self, refresh: Refresh | Transaction):
        """
        Cancel a running dataset or dataflow refresh operation.

        Args:
            refresh (Refresh | Transaction): The refresh instance to cancel.
        """
        is_dataflow = isinstance(refresh, Transaction)
        refreshable = refresh.dataflow if is_dataflow else refresh.dataset
        uri = f"[{refreshable.__class__.__name__}][{refreshable.group.name}/{refreshable.name}]"  # type: ignore
        if not refresh.is_in_progress:
            log.error(f"{uri} failed to cancel a refresh which is not running")
            return
        request_method = self.post if is_dataflow else self.delete
        await request_method(refresh.cancel_url)
        await asyncio.sleep(POST_CANCEL_DELAY)
        log.info(f"{uri} [{refresh.id}] cancelled.")

    async def get_last_refresh(
        self, refreshable: Dataflow | Dataset
    ) -> Refresh | Transaction | None:
        """
        Get the most recent refresh record for a dataset or dataflow.

        Args:
            refreshable (Dataset | Dataflow): The target object.

        Returns:
            Refresh | Transaction | None: The most recent refresh, if any.
        """
        history = list(await self.list_refresh_history(refreshable))
        if not history:
            return None
        sorted_history = sorted(history, key=lambda x: x.startTime)
        return sorted_history[-1]

    @Retry.on_conflict()
    async def refresh(
        self,
        refreshable: Dataflow | Dataset,
        refresh_type: Literal["Full", "Calculate"] = "Full",
        force: bool = False,
        wait_until_complete: bool = False,
    ):
        """
        Trigger a refresh operation for a dataset or dataflow, optionally waiting for completion.

        Args:
            refreshable (Dataset | Dataflow): The object to refresh.
            refresh_type (str): Type of refresh (Full or Calculate).
            force (bool): Whether to cancel ongoing refresh before starting a new one.
            wait_until_complete (bool): Whether to wait for refresh to complete before returning.
        """

        async def submit():
            kwargs = {}
            if isinstance(refreshable, Dataset):
                kwargs.update(
                    {"json": {"retryCount": 3, "type": refresh_type}}
                )
            elif isinstance(refreshable, Dataflow):
                kwargs.update({"json": {"refreshRequest": ""}})
            log.info(f"{uri} start to refresh.")

            trigger_time = DatetimeHelper.get_current_datetime()
            await self.post(refreshable.start_refresh_url, **kwargs)
            if wait_until_complete:
                await asyncio.sleep(PULL_STATUS_INTERVAL)
                waiting = wait_until_complete
                while waiting:
                    refresh_history = await self.list_refresh_history(
                        refreshable
                    )
                    last = sorted(
                        filter(
                            lambda x: x.startTime > trigger_time,
                            refresh_history,
                        ),
                        key=lambda x: x.startTime,
                    )[0]
                    if last is not None:
                        if last.is_in_progress:
                            duration = (
                                DatetimeHelper.get_current_datetime()
                                - last.startTime
                            )
                            log.debug(
                                f"{uri} {last.status} duration: {duration} s"
                            )
                            await asyncio.sleep(PULL_STATUS_INTERVAL)
                        else:
                            log.info(
                                f"{uri} stopped. {last.status} duration: {last.duration} s"
                            )
                            waiting = False

        last = await self.get_last_refresh(refreshable)
        uri = f"[{refreshable.__class__.__name__}][{refreshable.group.name}/{refreshable.name}]"  # type: ignore
        if last is not None and last.is_in_progress:
            if force:
                log.warning(
                    f"{uri} running task is going to be forcely cancelled to process a new one."
                )
                await self.cancel_refresh(last)
            else:
                log.info(f"{uri} skipped, a refresh is in progress.")
                return
        return await submit()

    async def list_schedules(self, *datasets: Dataset):
        """
        List refresh schedules for one or more datasets.

        Args:
            datasets (Dataset): One or more dataset objects.

        Returns:
            List[Schedule]: A list of schedule model instances.
        """
        filtered = [dataset for dataset in datasets if dataset.isRefreshable]
        tasks = [
            asyncio.create_task(self.get(dataset.get_schedule_url))
            for dataset in filtered
        ]
        responses = await asyncio.gather(*tasks)
        results = []
        for dataset, response in zip(filtered, responses):
            data = await response.json()
            data = {k: v for k, v in data.items() if k != "@odata.context"}  # type: ignore
            results.append(
                Schedule(
                    **data,
                    client=self.client,
                    group=dataset.group,
                    dataset=dataset,
                )
            )
        return results

    async def list_pages(self, *reports: Report):
        """
        List report pages (tabs) for Power BI reports.

        Args:
            reports (Report): One or more Report instances.

        Returns:
            List[Page]: A flat list of Page model instances.
        """
        filtered = [
            report
            for report in reports
            if report.reportType == "PowerBIReport"
        ]
        tasks = [
            asyncio.create_task(self.get(report.list_pages_url))
            for report in filtered
        ]
        responses = await asyncio.gather(*tasks)
        results = []
        for report, response in zip(filtered, responses):
            data = await response.json()
            pages = (
                Page(
                    **_, client=self.client, report=report, group=report.group
                )
                for _ in data["value"]
            )
            results.append(pages)
        return list(chain(*results))

    async def get_group(self, group_name: str):
        """
        Find a group (workspace) by its name.

        Args:
            group_name (str): The name of the group to locate.

        Returns:
            Group: The matched Group instance.
        """
        return next(
            g for g in await self.list_groups() if g.name == group_name
        )

    async def get_dataset(self, group_name: str, dataset_name: str):
        """
        Find a dataset by group name and dataset name.

        Args:
            group_name (str): The workspace name.
            dataset_name (str): The dataset name.

        Returns:
            Dataset: The matched Dataset instance.
        """
        group = await self.get_group(group_name)
        datasets = await self.list_datasets(group)
        return next(
            dataset for dataset in datasets if dataset.name == dataset_name
        )

    async def get_dataflow(self, group_name: str, dataflow_name: str):
        """
        Find a dataflow by group name and dataflow name.

        Args:
            group_name (str): The workspace name.
            dataflow_name (str): The dataflow name.

        Returns:
            Dataflow: The matched Dataflow instance.
        """
        group = await self.get_group(group_name)
        dataflows = await self.list_dataflows(group)
        return next(
            dataflow
            for dataflow in dataflows
            if dataflow.name == dataflow_name
        )

    async def get_report(self, group_name: str, report_name: str):
        """
        Find a report by group name and report name.

        Args:
            group_name (str): The workspace name.
            report_name (str): The report name.

        Returns:
            Report: The matched Report instance.
        """
        group = await self.get_group(group_name)
        reports = await self.list_reports(group)
        return next(report for report in reports if report.name == report_name)
