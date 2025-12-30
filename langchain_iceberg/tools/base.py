"""Base tool classes for Iceberg tools."""

from typing import Any, Optional

from langchain_core.tools import BaseTool
from pydantic import Field, PrivateAttr


class IcebergBaseTool(BaseTool):
    """Base class for all Iceberg tools."""

    _catalog: Any = PrivateAttr()

    def __init__(self, catalog: Any, **kwargs: Any):
        """Initialize base tool with catalog."""
        super().__init__(**kwargs)
        self._catalog = catalog

    @property
    def catalog(self) -> Any:
        """Get the catalog instance."""
        return self._catalog

