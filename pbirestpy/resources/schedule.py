from typing import List, TYPE_CHECKING, Optional
from .base import BaseResource

if TYPE_CHECKING:
    from .dataset import Dataset


class Schedule(BaseResource):
    __SLOTS__ = (
        "days",
        "times",
        "enabled",
        "localTimeZoneId",
        "notifyOption",
        "dataset.id",
        "dataset.name",
        "dataset.group.name",
        "dataset.group.id",
    )

    def __init__(
        self,
        days: List[str],
        times: List[str],
        enabled: bool = True,
        localTimeZoneId: Optional[str] = None,
        notifyOption: Optional[str] = None,
        dataset: Optional["Dataset"] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.days = days
        self.times = times
        self.enabled = enabled
        self.localTimeZoneId = localTimeZoneId
        self.notifyOption = notifyOption
        self.dataset = dataset
