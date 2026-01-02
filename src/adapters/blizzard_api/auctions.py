"""Blizzard API client for Auction House data."""

import asyncio
from typing import Optional

from adapters.blizzard_api.base import BlizzardAPIClient
from domain.models import AuctionData, ConnectedRealm
from ports.auction_house import AuctionHousePort


class AuctionsClient(BlizzardAPIClient, AuctionHousePort):
    """Client for fetching World of Warcraft Auction House data from Battle.net API.

    This adapter implements the AuctionHousePort interface, providing access to
    Blizzard's Battle.net API for auction house data.

    Usage:
        async with AuctionsClient(client_id, client_secret) as client:
            realm_ids = await client.get_connected_realm_ids()
            auctions = await client.get_auctions(realm_ids[0])
    """

    async def get_connected_realm_ids(self) -> list[int]:
        """Get list of all connected realm IDs.

        Returns:
            List of connected realm IDs.
        """
        data = await self._get("/data/wow/connected-realm/index")

        realm_ids = []
        for realm in data.get("connected_realms", []):
            href = realm.get("href", "")
            realm_id = href.split("/")[-1].split("?")[0] if href else None
            if realm_id and realm_id.isdigit():
                realm_ids.append(int(realm_id))

        return realm_ids

    async def get_connected_realm(self, realm_id: int) -> Optional[ConnectedRealm]:
        """Get details for a specific connected realm.

        Args:
            realm_id: The connected realm ID.

        Returns:
            ConnectedRealm object or None if not found.
        """
        data = await self._get(f"/data/wow/connected-realm/{realm_id}")
        return ConnectedRealm.from_api_response(realm_id, data)

    async def get_auctions(self, realm_id: int) -> AuctionData:
        """Get auction data for a specific connected realm.

        Args:
            realm_id: The connected realm ID.

        Returns:
            AuctionData containing all auctions for the realm.
        """
        data = await self._get(f"/data/wow/connected-realm/{realm_id}/auctions")
        return AuctionData.from_api_response(data, realm_id)

    async def get_commodity_auctions(self) -> AuctionData:
        """Get region-wide commodity auction data.

        Returns:
            AuctionData containing all commodity auctions.
        """
        data = await self._get("/data/wow/auctions/commodities")
        return AuctionData.from_api_response(data)

    async def get_multiple_realm_auctions(self, realm_ids: list[int], max_concurrent: int = 5) -> dict[int, AuctionData]:
        """Fetch auctions for multiple realms concurrently.

        Args:
            realm_ids: List of connected realm IDs.
            max_concurrent: Maximum number of concurrent requests.

        Returns:
            Dictionary mapping realm_id to AuctionData.
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_with_semaphore(realm_id: int) -> tuple[int, Optional[AuctionData]]:
            async with semaphore:
                try:
                    print(f"Fetching auctions for realm {realm_id}...")
                    data = await self.get_auctions(realm_id)
                    print(f"Completed realm {realm_id}: {len(data.auctions)} auctions")
                    return realm_id, data
                except Exception as e:
                    print(f"Error fetching realm {realm_id}: {e}")
                    return realm_id, None

        tasks = [fetch_with_semaphore(realm_id) for realm_id in realm_ids]
        results = await asyncio.gather(*tasks)

        return {realm_id: data for realm_id, data in results if data is not None}

    async def get_all_realm_details(self, realm_ids: list[int], max_concurrent: int = 10) -> dict[int, ConnectedRealm]:
        """Fetch details for multiple realms concurrently.

        Args:
            realm_ids: List of connected realm IDs.
            max_concurrent: Maximum number of concurrent requests.

        Returns:
            Dictionary mapping realm_id to ConnectedRealm.
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_with_semaphore(
            realm_id: int,
        ) -> tuple[int, Optional[ConnectedRealm]]:
            async with semaphore:
                try:
                    return realm_id, await self.get_connected_realm(realm_id)
                except Exception as e:
                    print(f"Error fetching realm {realm_id} details: {e}")
                    return realm_id, None

        tasks = [fetch_with_semaphore(realm_id) for realm_id in realm_ids]
        results = await asyncio.gather(*tasks)

        return {realm_id: data for realm_id, data in results if data is not None}
