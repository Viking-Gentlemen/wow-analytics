"""Blizzard API client for Item data."""

from typing import Any, Optional

from adapters.blizzard_api.base import BlizzardAPIClient
from domain.models import Item, ItemMedia
from ports.items import ItemsPort


class ItemsClient(BlizzardAPIClient, ItemsPort):
    """Client for fetching World of Warcraft Item data from Battle.net API.

    Usage:
        async with ItemsClient(client_id, client_secret) as client:
            item = await client.get_item(19019)
            print(f"{item.name} - {item.quality}")
    """

    async def get_item(self, item_id: int) -> Item:
        """Get item data by ID.

        Args:
            item_id: The item ID.

        Returns:
            Item object with item details.
        """
        data = await self._get(
            f"/data/wow/item/{item_id}",
            namespace=self.namespace_static,
        )
        return Item.from_api_response(data)

    async def get_item_media(self, item_id: int) -> ItemMedia:
        """Get item media (icon) by ID.

        Args:
            item_id: The item ID.

        Returns:
            ItemMedia object with icon URL.
        """
        data = await self._get(
            f"/data/wow/media/item/{item_id}",
            namespace=self.namespace_static,
        )
        return ItemMedia.from_api_response(item_id, data)

    async def get_item_class_index(self) -> list[dict[str, Any]]:
        """Get index of all item classes.

        Returns:
            List of item class references.
        """
        data = await self._get(
            "/data/wow/item-class/index",
            namespace=self.namespace_static,
        )
        return data.get("item_classes", [])

    async def get_item_class(self, class_id: int) -> dict[str, Any]:
        """Get details for a specific item class.

        Args:
            class_id: The item class ID.

        Returns:
            Item class details including subclasses.
        """
        return await self._get(
            f"/data/wow/item-class/{class_id}",
            namespace=self.namespace_static,
        )

    async def get_item_subclass(self, class_id: int, subclass_id: int) -> dict[str, Any]:
        """Get details for a specific item subclass.

        Args:
            class_id: The item class ID.
            subclass_id: The item subclass ID.

        Returns:
            Item subclass details.
        """
        return await self._get(
            f"/data/wow/item-class/{class_id}/item-subclass/{subclass_id}",
            namespace=self.namespace_static,
        )

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
        params = {
            "orderby": order_by,
            "_page": page,
        }
        if name:
            params["name.en_US"] = name

        return await self._get(
            "/data/wow/search/item",
            params=params,
            namespace=self.namespace_static,
        )
