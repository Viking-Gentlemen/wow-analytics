"""Port (interface) for auction house data retrieval."""

from abc import ABC, abstractmethod
from typing import Optional

from domain.models import AuctionData, ConnectedRealm


class AuctionHousePort(ABC):
    """Abstract interface for auction house data retrieval.

    This port defines the contract for fetching auction house data
    from any source (Blizzard API, mock data, cache, etc.).
    """

    @abstractmethod
    async def __aenter__(self) -> "AuctionHousePort":
        """Async context manager entry."""
        ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        ...

    @abstractmethod
    async def get_connected_realm_ids(self) -> list[int]:
        """Get list of all connected realm IDs.

        Returns:
            List of connected realm IDs.
        """
        ...

    @abstractmethod
    async def get_connected_realm(self, realm_id: int) -> Optional[ConnectedRealm]:
        """Get details for a specific connected realm.

        Args:
            realm_id: The connected realm ID.

        Returns:
            ConnectedRealm object or None if not found.
        """
        ...

    @abstractmethod
    async def get_auctions(self, realm_id: int) -> AuctionData:
        """Get auction data for a specific connected realm.

        Args:
            realm_id: The connected realm ID.

        Returns:
            AuctionData containing all auctions for the realm.
        """
        ...

    @abstractmethod
    async def get_commodity_auctions(self) -> AuctionData:
        """Get region-wide commodity auction data.

        Returns:
            AuctionData containing all commodity auctions.
        """
        ...

    @abstractmethod
    async def get_multiple_realm_auctions(
        self, realm_ids: list[int], max_concurrent: int = 5
    ) -> dict[int, AuctionData]:
        """Fetch auctions for multiple realms concurrently.

        Args:
            realm_ids: List of connected realm IDs.
            max_concurrent: Maximum number of concurrent requests.

        Returns:
            Dictionary mapping realm_id to AuctionData.
        """
        ...

    @abstractmethod
    async def get_all_realm_details(
        self, realm_ids: list[int], max_concurrent: int = 10
    ) -> dict[int, ConnectedRealm]:
        """Fetch details for multiple realms concurrently.

        Args:
            realm_ids: List of connected realm IDs.
            max_concurrent: Maximum number of concurrent requests.

        Returns:
            Dictionary mapping realm_id to ConnectedRealm.
        """
        ...
