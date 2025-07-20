from enum import Enum
from typing import TYPE_CHECKING, Optional
from ..utils import DatetimeHelper

if TYPE_CHECKING:
    from ..client import PowerBIClient
    from .group import Group


class BaseResource:
    """
    Base class for all resources in the Power BI REST API client.
    This class provides common functionality that can be shared across different resource classes.
    """

    BASE_URL = "https://api.powerbi.com/v1.0/myorg"
    __SLOTS__ = ("id",)

    def __init__(
        self,
        client: "PowerBIClient",
        id: Optional[str] = None,
        name: Optional[str] = None,
        group: Optional["Group"] = None,
    ):
        """
        Initializes the BaseResource with a client instance.

        Args:
            client: An instance of the Power BI REST API client.
        """

        self.client = client
        self.id = id
        self.name = name
        self.group = group
        self.group_id = group.id if group else self.id

    def __repr__(self):
        """
        Returns a string representation of the resource.
        This representation includes the class name and the id of the resource.
        """
        cls_name = self.__class__.__name__
        attrs = "\n\t".join(
            [f"{key}={v}" for key, v in self.to_dict().items()]
        )
        return f"{cls_name}(\n\t{attrs}\n)"

    def _search_slot(self, slot: str):
        paths = slot.split(".")
        value = self
        for path in paths:
            if hasattr(value, path):
                value = getattr(value, path)
            else:
                return None
        return str(value)

    def to_dict(self):
        """
        Converts the resource to a dictionary representation.

        Returns:
            dict: A dictionary representation of the resource (only keys defined in __SLOTS__).
        """
        return {
            slot.replace(".", "_"): self._search_slot(slot)
            for slot in self.__SLOTS__
        }

    @staticmethod
    def build_url(path: str):
        """
        Builds a full URL for the resource based on the base URL and the provided path.

        Args:
            path (str): The specific path for the resource.

        Returns:
            str: The full URL for the resource.
        """
        return f"{BaseResource.BASE_URL}/{path.lstrip('/')}"


class RefreshStatus(Enum):
    """
    Enum representing the status of a refresh operation.
    """

    InProgress = "InProgress"
    Completed = "Completed"
    Failed = "Failed"
    Pending = "Pending"
    Cancelled = "Cancelled"
    Total = "Total"

    def __str__(self):
        return self.name

    @classmethod
    def from_str(cls, value: str) -> "RefreshStatus":
        if value.lower() == "success":
            value = cls.Completed.value
        if value.lower() == "unknown":
            value = cls.InProgress.value
        if value.lower() == "cancelling":
            value = cls.Cancelled.value
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        return cls.Failed  # fallback to failed


class BaseRefresh(BaseResource):
    """
    Base class for resources that support refresh operations.
    This class provides common functionality for handling refresh operations.
    """

    def __init__(
        self,
        startTime: str,
        status: str,
        endTime: Optional[str] = None,
        *args,
        **kwargs,
    ):
        """
        Initializes the BaseRefresh with start time, status, and optional end time.

        Args:
            startTime (str): The start time of the refresh operation.
            status (str): The status of the refresh operation.
            endTime (Optional[str]): The end time of the refresh operation.
        """
        super().__init__(*args, **kwargs)
        self.startTime = DatetimeHelper.parse_datetime(startTime)
        self.endTime = DatetimeHelper.parse_datetime(endTime)
        self.status = RefreshStatus.from_str(status)
        self.duration = self.endTime - self.startTime if endTime else None

    @property
    def is_in_progress(self) -> bool:
        """
        Checks if the refresh operation is currently in progress.

        Returns:
            bool: True if the refresh operation is in progress, False otherwise.
        """
        return self.status == RefreshStatus.InProgress


class BaseRefreshable(BaseResource):
    """
    Base class for resources that can be refreshed.
    This class provides common functionality for handling refreshable resources.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the BaseRefreshable with the provided arguments.
        """
        super().__init__(*args, **kwargs)
