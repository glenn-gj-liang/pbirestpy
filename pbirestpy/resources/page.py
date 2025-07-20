from typing import TYPE_CHECKING
from .base import BaseResource

if TYPE_CHECKING:
    from .report import Report


class Page(BaseResource):
    __SLOTS__ = (
        "id",
        "name",
        "order",
        "report.name",
        "report.id",
        "report.group_id",
    )

    def __init__(
        self,
        displayName: str,
        order: int,
        report: "Report",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.name = displayName
        self.id = order
        self.displayName = displayName
        self.order = order
        self.report = report
