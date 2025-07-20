from typing import Optional
from .base import BaseResource


class Transaction(BaseResource):
    """
    Represents a transaction resource in the PBI REST API.
    """

    __SLOTS__ = (
        "id",
        "startTime",
        "endTime",
        "refreshType",
        "status",
        "errorInfo",
        "dataflow.id",
        "dataflow.name",
        "dataflow.group_id",
    )

    def __init__(
        self,
        refreshType: str,
        dataflow,
        errorInfo: Optional[dict] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.refreshType = refreshType
        self.dataflow = dataflow
        self.errorInfo = errorInfo

    @property
    def cancel_url(self) -> str:
        """
        Returns the URL to cancel the transaction.
        """
        return self.build_url(
            f"groups/{self.dataflow.group_id}/dataflows/transactions/{self.id}/cancel"
        )
