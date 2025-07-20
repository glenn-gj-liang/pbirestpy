import asyncio
from itertools import chain
from typing import Dict, List
from aiohttp import ClientSession, ClientTimeout, ClientResponseError
from .client import PowerBIClient
from ..resources import Group, Dataset, Dataflow, Refresh, Transaction

DEFAULT_CLIENT_TIMEOUT = ClientTimeout(total=600)


class BaseSession:
    def __init__(self, client: "PowerBIClient"):
        self.client: "PowerBIClient" = client
        self._session: ClientSession = None  # type: ignore
        self._timeout: ClientTimeout = DEFAULT_CLIENT_TIMEOUT

    def _build_headers(self, kwargs: Dict) -> Dict[str, str]:
        headers = kwargs.get("headers", {})
        authorization = {"Authorization": self.client.authenticator.token}
        headers.update(authorization)
        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"
        return headers

    async def init(self):
        await self._ensure_session()

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = ClientSession(timeout=self._timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, method: str, url: str, **kwargs):
        await self._ensure_session()
        headers = self._build_headers(kwargs)
        kwargs["headers"] = headers
        response = await self._session.request(method, url, **kwargs)
        return response

    async def get(self, url: str, **kwargs):
        return await self._request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs):
        return await self._request("POST", url, **kwargs)

    async def delete(self, url: str, **kwargs):
        return await self._request("DELETE", url, **kwargs)


class ApiSession(BaseSession):
    async def list_groups(self):
        """
        Lists all Power BI Groups (Workspaces).
        Returns:
            List[Group]: A list of Group objects representing the Power BI Groups.
        """

        response = await self.get(Group.LIST_GROUPS_URL)
        data = await response.json()["value"]  # type: ignore
        return [Group(**_, client=self.client) for _ in data]

    async def _list_elements_in_groups(self, *groups, attr: str, element_type):
        """
        Lists elements (datasets, reports, etc.) in the specified groups.
        Args:
            groups: A variable number of Group objects.
            attr: The attribute name to fetch from each group (e.g., 'datasets', 'reports').
            element_type: The type of elements to return (e.g., Dataset, Report).
        Returns:
            List[element_type]: A list of elements of the specified type from the groups.
        """

        attr = f"list_{attr}_url"
        tasks = [
            asyncio.create_task(self.get(getattr(group, attr)))
            for group in groups
        ]
        responses = await asyncio.gather(*tasks)
        elements = []
        for group, response in zip(groups, responses):
            data = await response.json()["value"]  # type: ignore
            in_group = [
                element_type(**_, client=self.client, group=group)
                for _ in data
            ]
            elements.extend(in_group)
        return elements

    async def list_datasets(self, *groups: Group) -> List:
        """
        Lists datasets in the specified Power BI Groups (Workspaces).
        Args:
            groups: A variable number of Group objects.
        Returns:
            List[Dataset]: A list of Dataset objects from the specified groups.
        """
        return await self._list_elements_in_groups(
            *groups, attr="datasets", element_type=Dataset
        )

    async def list_dataflows(self, *groups: Group) -> List:
        """
        Lists dataflows in the specified Power BI Groups (Workspaces).
        Args:
            groups: A variable number of Group objects.
        Returns:
            List[Dataflow]: A list of Dataflow objects from the specified groups.
        """
        return await self._list_elements_in_groups(
            *groups, attr="dataflows", element_type=Dataflow
        )

    async def list_refreshes(self, *datasets: Dataset):
        async def _(dataset: Dataset):
            response = await self.get(dataset.list_refreshes_url)
            data = await response.json()
            return [
                Refresh(
                    **_,
                    client=self.client,
                    dataset=dataset,
                    group=dataset.group,
                )
                for _ in data["value"]
            ]

        tasks = [asyncio.create_task(_(dataset)) for dataset in datasets]
        return chain(await asyncio.gather(*tasks))
