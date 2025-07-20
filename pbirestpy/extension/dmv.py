from ..resources import *
from ..client import PowerBIClient
from ..utils import RuntimeHelper


class DMV:
    def __init__(self, client: "PowerBIClient") -> None:
        self.client = client

    @RuntimeHelper.to_sparkdf
    async def groups(self):
        async with self.client as session:
            groups = await session.list_groups()
            return self.client.to_df(groups)

    @RuntimeHelper.to_sparkdf
    async def datasets(self):
        async with self.client as session:
            groups = await session.list_groups()
            datasets = await session.list_datasets(*groups)
            return self.client.to_df(datasets)
