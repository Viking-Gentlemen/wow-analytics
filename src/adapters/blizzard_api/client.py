"""Blizzard API client adapter for fetching WoW Auction House data."""

import asyncio
import time
from typing import Optional

from authlib.integrations.httpx_client import AsyncOAuth2Client

from domain.models import AuctionData, ConnectedRealm
from ports.auction_house import AuctionHousePort


class BlizzardAuctionHouseClient(AuctionHousePort):
    """Client for fetching World of Warcraft Auction House data from Battle.net API.

    This adapter implements the AuctionHousePort interface, providing access to
    Blizzard's Battle.net API for auction house data.

    Usage:
        async with BlizzardAuctionHouseClient(client_id, client_secret) as client:
            realm_ids = await client.get_connected_realm_ids()
            auctions = await client.get_auctions(realm_ids[0])
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        region: str = "us",
        locale: str = "en_US",
    ):
        """Initialize the client with OAuth credentials.

        Args:
            client_id: Your Battle.net API client ID.
            client_secret: Your Battle.net API client secret.
            region: API region ('us', 'eu', 'kr', 'tw', 'cn').
            locale: Locale for localized strings.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.region = region.lower()
        self.locale = locale

        # Set region-specific endpoints
        self.token_url = f"https://{self.region}.battle.net/oauth/token"
        self.api_base = f"https://{self.region}.api.blizzard.com"

        # Namespaces
        self.namespace_static = f"static-{self.region}"
        self.namespace_dynamic = f"dynamic-{self.region}"

        # HTTP default params
        self.default_params = {
            "namespace": self.namespace_dynamic,
            "locale": self.locale,
        }

        self.client: Optional[AsyncOAuth2Client] = None
        self.token: Optional[dict] = None

    async def __aenter__(self) -> "BlizzardAuctionHouseClient":
        """Async context manager entry."""
        await self._authenticate()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        if self.client:
            await self.client.aclose()

    async def _authenticate(self) -> None:
        """Authenticate and get access token using client credentials flow."""
        try:
            self.client = AsyncOAuth2Client(
                client_id=self.client_id,
                client_secret=self.client_secret,
                token_endpoint=self.token_url,
            )

            self.token = await self.client.fetch_token(
                self.token_url,
                grant_type="client_credentials",
            )
            print(
                f"Successfully authenticated. Token expires in {self.token.get('expires_in')} seconds"
            )
        except Exception as e:
            print(f"Authentication failed: {e}")
            raise

    async def _ensure_valid_token(self) -> None:
        """Check if token is valid and refresh if necessary."""
        if not self.token or self.token.get("expires_at", 0) <= time.time():
            print("Token expired or missing, re-authenticating...")
            await self._authenticate()

    async def get_connected_realm_ids(self) -> list[int]:
        """Get list of all connected realm IDs.

        Returns:
            List of connected realm IDs.
        """
        await self._ensure_valid_token()

        url = f"{self.api_base}/data/wow/connected-realm/index"

        response = await self.client.get(url, params=self.default_params)
        response.raise_for_status()

        realm_ids = []
        for realm in response.json().get("connected_realms", []):
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
        await self._ensure_valid_token()

        url = f"{self.api_base}/data/wow/connected-realm/{realm_id}"

        response = await self.client.get(url, params=self.default_params)
        response.raise_for_status()

        return ConnectedRealm.from_api_response(realm_id, response.json())

    async def get_auctions(self, realm_id: int) -> AuctionData:
        """Get auction data for a specific connected realm.

        Args:
            realm_id: The connected realm ID.

        Returns:
            AuctionData containing all auctions for the realm.
        """
        await self._ensure_valid_token()

        url = f"{self.api_base}/data/wow/connected-realm/{realm_id}/auctions"

        response = await self.client.get(url, params=self.default_params)
        response.raise_for_status()

        return AuctionData.from_api_response(response.json(), realm_id)

    async def get_commodity_auctions(self) -> AuctionData:
        """Get region-wide commodity auction data.

        Returns:
            AuctionData containing all commodity auctions.
        """
        await self._ensure_valid_token()

        url = f"{self.api_base}/data/wow/auctions/commodities"

        response = await self.client.get(url, params=self.default_params)
        response.raise_for_status()

        return AuctionData.from_api_response(response.json())

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
