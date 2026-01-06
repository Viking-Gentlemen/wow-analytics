"""
Blizzard API Port - Interface for fetching World of Warcraft game data.

This port defines the contract for accessing Blizzard's Battle.net API.
It provides methods for fetching auctions, items, recipes, and realm data.
The adapter implementation handles API authentication, rate limiting,
and transformation of raw API responses into domain models.
"""

from abc import ABC, abstractmethod
from typing import Optional

from domain.models import (
    AuctionData,
    ConnectedRealm,
    Item,
    ItemMedia,
    Profession,
    Recipe,
)


class BlizzardAPIPort(ABC):
    """
    Abstract interface for Blizzard API access.

    All methods are async to support efficient concurrent API calls.
    The adapter is responsible for:
    - OAuth2 authentication
    - Rate limiting
    - Parsing API responses into domain models
    """

    # =========================================================================
    # Context Manager
    # =========================================================================

    @abstractmethod
    async def __aenter__(self) -> "BlizzardAPIPort":
        """Enter async context - initialize HTTP client."""
        pass

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context - cleanup HTTP client."""
        pass

    # =========================================================================
    # Realm Methods
    # =========================================================================

    @abstractmethod
    async def get_connected_realm_ids(self) -> list[int]:
        """
        Fetch all connected realm IDs for the configured region.

        Returns:
            List of connected realm IDs.
        """
        pass

    @abstractmethod
    async def get_connected_realm(self, realm_id: int) -> Optional[ConnectedRealm]:
        """
        Fetch details for a specific connected realm.

        Args:
            realm_id: The connected realm ID.

        Returns:
            ConnectedRealm domain object, or None if not found.
        """
        pass

    @abstractmethod
    async def get_all_connected_realms(self, realm_ids: Optional[list[int]] = None, max_concurrent: int = 10) -> dict[int, ConnectedRealm]:
        """
        Fetch details for multiple connected realms concurrently.

        Args:
            realm_ids: List of realm IDs to fetch. If None, fetches all realms.
            max_concurrent: Maximum number of concurrent requests.

        Returns:
            Dictionary mapping realm_id to ConnectedRealm.
        """
        pass

    # =========================================================================
    # Auction Methods
    # =========================================================================

    @abstractmethod
    async def get_auctions(self, realm_id: int) -> AuctionData:
        """
        Fetch all auctions for a specific connected realm.

        Args:
            realm_id: The connected realm ID.

        Returns:
            AuctionData containing all auctions for the realm.
        """
        pass

    @abstractmethod
    async def get_commodity_auctions(self) -> AuctionData:
        """
        Fetch region-wide commodity auctions.

        Commodities are items that can be bought/sold across all realms
        in a region (e.g., crafting materials, consumables).

        Returns:
            AuctionData containing all commodity auctions.
        """
        pass

    @abstractmethod
    async def get_multiple_realm_auctions(self, realm_ids: list[int], max_concurrent: int = 5) -> dict[int, AuctionData]:
        """
        Fetch auctions from multiple realms concurrently.

        Args:
            realm_ids: List of realm IDs to fetch auctions from.
            max_concurrent: Maximum number of concurrent requests.

        Returns:
            Dictionary mapping realm_id to AuctionData.
        """
        pass

    # =========================================================================
    # Item Methods
    # =========================================================================

    @abstractmethod
    async def get_item(self, item_id: int) -> Optional[Item]:
        """
        Fetch details for a specific item.

        Args:
            item_id: The item ID.

        Returns:
            Item domain object, or None if not found.
        """
        pass

    @abstractmethod
    async def get_item_media(self, item_id: int) -> Optional[ItemMedia]:
        """
        Fetch media (icon) for a specific item.

        Args:
            item_id: The item ID.

        Returns:
            ItemMedia domain object, or None if not found.
        """
        pass

    @abstractmethod
    async def get_item_classes(self) -> list[dict]:
        """
        Fetch the item class index (categories like Weapon, Armor, etc.).

        Returns:
            List of item class dictionaries with id and name.
        """
        pass

    @abstractmethod
    async def search_items(
        self,
        name: Optional[str] = None,
        order_by: str = "id",
        page: int = 1,
    ) -> dict:
        """
        Search for items by name.

        Args:
            name: Item name to search for (partial match).
            order_by: Field to sort results by.
            page: Page number for pagination.

        Returns:
            Raw search results dictionary.
        """
        pass

    # =========================================================================
    # Recipe/Profession Methods
    # =========================================================================

    @abstractmethod
    async def get_professions(self) -> list[Profession]:
        """
        Fetch all professions.

        Returns:
            List of Profession domain objects.
        """
        pass

    @abstractmethod
    async def get_recipe(self, recipe_id: int) -> Optional[Recipe]:
        """
        Fetch details for a specific recipe.

        Args:
            recipe_id: The recipe ID.

        Returns:
            Recipe domain object, or None if not found.
        """
        pass

    @abstractmethod
    async def search_recipes(
        self,
        name: Optional[str] = None,
        order_by: str = "id",
        page: int = 1,
    ) -> dict:
        """
        Search for recipes by name.

        Args:
            name: Recipe name to search for (partial match).
            order_by: Field to sort results by.
            page: Page number for pagination.

        Returns:
            Raw search results dictionary.
        """
        pass
