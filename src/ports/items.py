"""Port (interface) for item data retrieval."""

from abc import ABC, abstractmethod
from typing import Any, Optional

from domain.models import Item, ItemMedia


class ItemsPort(ABC):
    """Abstract interface for item data retrieval.

    This port defines the contract for fetching item data
    from any source (Blizzard API, mock data, cache, etc.).
    """

    @abstractmethod
    async def __aenter__(self) -> "ItemsPort":
        """Async context manager entry."""
        ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        ...

    @abstractmethod
    async def get_item(self, item_id: int) -> Item:
        """Get item data by ID.

        Args:
            item_id: The item ID.

        Returns:
            Item object with item details.
        """
        ...

    @abstractmethod
    async def get_item_media(self, item_id: int) -> ItemMedia:
        """Get item media (icon) by ID.

        Args:
            item_id: The item ID.

        Returns:
            ItemMedia object with icon URL.
        """
        ...

    @abstractmethod
    async def get_item_class_index(self) -> list[dict[str, Any]]:
        """Get index of all item classes.

        Returns:
            List of item class references.
        """
        ...

    @abstractmethod
    async def get_item_class(self, class_id: int) -> dict[str, Any]:
        """Get details for a specific item class.

        Args:
            class_id: The item class ID.

        Returns:
            Item class details including subclasses.
        """
        ...

    @abstractmethod
    async def get_item_subclass(self, class_id: int, subclass_id: int) -> dict[str, Any]:
        """Get details for a specific item subclass.

        Args:
            class_id: The item class ID.
            subclass_id: The item subclass ID.

        Returns:
            Item subclass details.
        """
        ...

    @abstractmethod
    async def search_items(
        self,
        name: Optional[str] = None,
        order_by: str = "id",
        page: int = 1,
    ) -> dict[str, Any]:
        """Search for items.

        Args:
            name: Item name to search for (supports wildcards).
            order_by: Field to order results by.
            page: Page number for pagination.

        Returns:
            Search results with item references.
        """
        ...
