from typing import Optional, Dict
from .base import BaseResource


class Refresh(BaseResource):
    """
    Represents a Power BI Refresh operation.
    """

    __SLOTS__ = (
        "id",
        "status",
        "startTime",
        "endTime",
        "requestId",
        "refreshType",
        "refreshAttempts",
        "serviceExceptionJson",
        "extendedStatus",
        "dataset.id",
        "dataset.name",
        "dataset.group_id",
    )

    def __init__(
        self,
        requestId: str,
        refreshType: str,
        dataset,
        refreshAttempts: Optional[Dict] = None,
        serviceExceptionJson: Optional[Dict] = None,
        extendedStatus: Optional[str] = None,
        *args,
        **kwargs,
    ):
        """
        Initializes the Refresh with its properties.
        """
        super().__init__(*args, **kwargs)
        self.requestId = requestId
        self.refreshType = refreshType
        self.dataset = dataset
        self.refreshAttempts = refreshAttempts or {}
        self.serviceExceptionJson = serviceExceptionJson or {}
        self.extendedStatus = extendedStatus or ""
        self.id = self.requestId

    @property
    def cancel_url(self) -> str:
        """
        Returns the URL to cancel the refresh operation.

        Returns:
            str: The URL to cancel the refresh operation.
        """
        return BaseResource.build_url(
            f"groups/{self.dataset.group_id}/datasets/{self.dataset.id}/refreshes/{self.id}"
        )
