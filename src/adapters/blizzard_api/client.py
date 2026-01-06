"""
Blizzard API Client - Implementation of BlizzardAPIPort.

This adapter handles:
- OAuth2 authentication with Battle.net
- HTTP requests to the Blizzard API
- Rate limiting via semaphores
- Transformation of raw API responses into domain models

All API response parsing logic is encapsulated here, keeping the domain
layer pure and free from external data format knowledge.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

import httpx
from authlib.integrations.httpx_client import AsyncOAuth2Client

from domain.models import (
    Auction,
    AuctionData,
    AuctionItem,
    ConnectedRealm,
    Item,
    ItemMedia,
    PopulationType,
    Profession,
    RealmStatus,
    Recipe,
    RecipeReagent,
)
from ports.blizzard_api import BlizzardAPIPort


class BlizzardAPIClient(BlizzardAPIPort):
    """
    Blizzard Battle.net API client implementation.

    Handles OAuth2 authentication, request execution, and response parsing.
    All parsing logic is private to this adapter.
    """

    # Region-specific configuration
    REGION_CONFIG = {
        "us": {
            "token_url": "https://oauth.battle.net/token",
            "api_base": "https://us.api.blizzard.com",
        },
        "eu": {
            "token_url": "https://oauth.battle.net/token",
            "api_base": "https://eu.api.blizzard.com",
        },
        "kr": {
            "token_url": "https://oauth.battle.net/token",
            "api_base": "https://kr.api.blizzard.com",
        },
        "tw": {
            "token_url": "https://oauth.battle.net/token",
            "api_base": "https://tw.api.blizzard.com",
        },
        "cn": {
            "token_url": "https://oauth.battlenet.com.cn/token",
            "api_base": "https://gateway.battlenet.com.cn",
        },
    }

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        region: str = "eu",
        locale: str = "en_GB",
    ):
        """
        Initialize the Blizzard API client.

        Args:
            client_id: Battle.net API client ID.
            client_secret: Battle.net API client secret.
            region: API region (us, eu, kr, tw, cn).
            locale: Locale for response data (e.g., en_US, en_GB, fr_FR).
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.region = region.lower()
        self.locale = locale

        if self.region not in self.REGION_CONFIG:
            raise ValueError(f"Invalid region: {region}")

        config = self.REGION_CONFIG[self.region]
        self.token_url = config["token_url"]
        self.api_base = config["api_base"]

        self._client: Optional[AsyncOAuth2Client] = None
        self._token_expires_at: Optional[float] = None

    # =========================================================================
    # Context Manager
    # =========================================================================

    async def __aenter__(self) -> "BlizzardAPIClient":
        """Initialize the OAuth2 client."""
        self._client = AsyncOAuth2Client(
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        await self._authenticate()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    # =========================================================================
    # Authentication
    # =========================================================================

    async def _authenticate(self) -> None:
        """Obtain OAuth2 access token using client credentials flow."""
        token = await self._client.fetch_token(
            self.token_url,
            grant_type="client_credentials",
        )
        # Store expiration time with a small buffer
        self._token_expires_at = token.get("expires_at", 0) - 60

    async def _ensure_valid_token(self) -> None:
        """Re-authenticate if the token is expired or about to expire."""
        import time

        if self._token_expires_at is None or time.time() >= self._token_expires_at:
            await self._authenticate()

    # =========================================================================
    # HTTP Methods
    # =========================================================================

    async def _get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        namespace: str = "dynamic",
    ) -> dict:
        """
        Make an authenticated GET request to the API.

        Args:
            endpoint: API endpoint path (e.g., /data/wow/connected-realm/index).
            params: Optional query parameters.
            namespace: API namespace (dynamic or static).

        Returns:
            Parsed JSON response.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        await self._ensure_valid_token()

        full_namespace = f"{namespace}-{self.region}"
        request_params = {
            "namespace": full_namespace,
            "locale": self.locale,
            **(params or {}),
        }

        url = f"{self.api_base}{endpoint}"
        response = await self._client.get(url, params=request_params)
        response.raise_for_status()
        return response.json()

    # =========================================================================
    # Realm Methods
    # =========================================================================

    async def get_connected_realm_ids(self) -> list[int]:
        """Fetch all connected realm IDs for the region."""
        data = await self._get("/data/wow/connected-realm/index")
        realm_ids = []
        for realm in data.get("connected_realms", []):
            href = realm.get("href", "")
            # Extract ID from URL like ".../connected-realm/123?..."
            if "/connected-realm/" in href:
                realm_id = int(href.split("/connected-realm/")[1].split("?")[0])
                realm_ids.append(realm_id)
        return realm_ids

    async def get_connected_realm(self, realm_id: int) -> Optional[ConnectedRealm]:
        """Fetch details for a specific connected realm."""
        try:
            data = await self._get(f"/data/wow/connected-realm/{realm_id}")
            return self._parse_connected_realm(data)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    async def get_all_connected_realms(self, realm_ids: Optional[list[int]] = None, max_concurrent: int = 10) -> dict[int, ConnectedRealm]:
        """Fetch details for multiple connected realms concurrently."""
        if realm_ids is None:
            realm_ids = await self.get_connected_realm_ids()

        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_realm(rid: int) -> tuple[int, Optional[ConnectedRealm]]:
            async with semaphore:
                realm = await self.get_connected_realm(rid)
                return rid, realm

        tasks = [fetch_realm(rid) for rid in realm_ids]
        results = await asyncio.gather(*tasks)

        return {rid: realm for rid, realm in results if realm is not None}

    def _parse_connected_realm(self, data: dict) -> ConnectedRealm:
        """Parse API response into ConnectedRealm domain model."""
        realms = data.get("realms", [])
        return ConnectedRealm(
            id=data["id"],
            realm_names=tuple(r.get("name", "") for r in realms),
            realm_slugs=tuple(r.get("slug", "") for r in realms),
            status=RealmStatus(data.get("status", {}).get("type", "UP")),
            population=PopulationType(data.get("population", {}).get("type", "MEDIUM")),
            has_queue=data.get("has_queue", False),
        )

    # =========================================================================
    # Auction Methods
    # =========================================================================

    async def get_auctions(self, realm_id: int) -> AuctionData:
        """Fetch all auctions for a specific connected realm."""
        data = await self._get(f"/data/wow/connected-realm/{realm_id}/auctions")
        return self._parse_auction_data(data, realm_id)

    async def get_commodity_auctions(self) -> AuctionData:
        """Fetch region-wide commodity auctions."""
        data = await self._get("/data/wow/auctions/commodities")
        # Commodity auctions use realm_id 0 as a convention
        return self._parse_auction_data(data, connected_realm_id=0)

    async def get_multiple_realm_auctions(self, realm_ids: list[int], max_concurrent: int = 5) -> dict[int, AuctionData]:
        """Fetch auctions from multiple realms concurrently."""
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_auctions(rid: int) -> tuple[int, AuctionData]:
            async with semaphore:
                auctions = await self.get_auctions(rid)
                return rid, auctions

        tasks = [fetch_auctions(rid) for rid in realm_ids]
        results = await asyncio.gather(*tasks)

        return {rid: data for rid, data in results}

    def _parse_auction_data(self, data: dict, connected_realm_id: int) -> AuctionData:
        """Parse API response into AuctionData domain model."""
        auctions = []
        for auction_dict in data.get("auctions", []):
            item_data = auction_dict.get("item", {})
            item = AuctionItem(
                id=item_data.get("id", 0),
                bonus_lists=tuple(item_data.get("bonus_lists", [])),
                modifiers=tuple((m.get("type", 0), m.get("value", 0)) for m in item_data.get("modifiers", [])),
            )
            auction = Auction(
                id=auction_dict.get("id", 0),
                item=item,
                quantity=auction_dict.get("quantity", 1),
                time_left=auction_dict.get("time_left", "UNKNOWN"),
                unit_price=auction_dict.get("unit_price"),
                buyout=auction_dict.get("buyout"),
                bid=auction_dict.get("bid"),
            )
            auctions.append(auction)

        return AuctionData(
            connected_realm_id=connected_realm_id,
            auctions=auctions,
            fetch_timestamp=datetime.now(timezone.utc),
        )

    # =========================================================================
    # Item Methods
    # =========================================================================

    async def get_item(self, item_id: int) -> Optional[Item]:
        """Fetch details for a specific item."""
        try:
            data = await self._get(f"/data/wow/item/{item_id}", namespace="static")
            return self._parse_item(data)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    async def get_item_media(self, item_id: int) -> Optional[ItemMedia]:
        """Fetch media (icon) for a specific item."""
        try:
            data = await self._get(f"/data/wow/media/item/{item_id}", namespace="static")
            return self._parse_item_media(item_id, data)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    async def get_item_classes(self) -> list[dict]:
        """Fetch the item class index."""
        data = await self._get("/data/wow/item-class/index", namespace="static")
        return [{"id": cls.get("id"), "name": cls.get("name", "")} for cls in data.get("item_classes", [])]

    async def search_items(
        self,
        name: Optional[str] = None,
        order_by: str = "id",
        page: int = 1,
    ) -> dict:
        """Search for items by name."""
        params = {"orderby": order_by, "_page": page}
        if name:
            params["name.en_US"] = name
        return await self._get("/data/wow/search/item", params=params, namespace="static")

    def _parse_item(self, data: dict) -> Item:
        """Parse API response into Item domain model."""
        return Item(
            id=data.get("id", 0),
            name=data.get("name", ""),
            quality=data.get("quality", {}).get("type", "COMMON"),
            level=data.get("level", 0),
            item_class=data.get("item_class", {}).get("name", ""),
            item_subclass=data.get("item_subclass", {}).get("name", ""),
            inventory_type=data.get("inventory_type", {}).get("type", ""),
            purchase_price=data.get("purchase_price", 0),
            sell_price=data.get("sell_price", 0),
            max_count=data.get("max_count", 0),
            is_equippable=data.get("is_equippable", False),
            is_stackable=data.get("is_stackable", False),
            description=data.get("description", ""),
        )

    def _parse_item_media(self, item_id: int, data: dict) -> ItemMedia:
        """Parse API response into ItemMedia domain model."""
        assets = data.get("assets", [])
        icon_url = ""
        for asset in assets:
            if asset.get("key") == "icon":
                icon_url = asset.get("value", "")
                break
        return ItemMedia(item_id=item_id, icon_url=icon_url)

    # =========================================================================
    # Recipe/Profession Methods
    # =========================================================================

    async def get_professions(self) -> list[Profession]:
        """Fetch all professions."""
        data = await self._get("/data/wow/profession/index", namespace="static")
        professions = []
        for prof in data.get("professions", []):
            professions.append(
                Profession(
                    id=prof.get("id", 0),
                    name=prof.get("name", ""),
                    type="PRIMARY",  # API doesn't distinguish, would need additional logic
                )
            )
        return professions

    async def get_recipe(self, recipe_id: int) -> Optional[Recipe]:
        """Fetch details for a specific recipe."""
        try:
            data = await self._get(f"/data/wow/recipe/{recipe_id}", namespace="static")
            return self._parse_recipe(data)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    async def search_recipes(
        self,
        name: Optional[str] = None,
        order_by: str = "id",
        page: int = 1,
    ) -> dict:
        """Search for recipes by name."""
        params = {"orderby": order_by, "_page": page}
        if name:
            params["name.en_US"] = name
        return await self._get("/data/wow/search/recipe", params=params, namespace="static")

    def _parse_recipe(self, data: dict) -> Recipe:
        """Parse API response into Recipe domain model."""
        reagents = []
        for reagent_data in data.get("reagents", []):
            reagent_item = reagent_data.get("reagent", {})
            reagents.append(
                RecipeReagent(
                    item_id=reagent_item.get("id", 0),
                    item_name=reagent_item.get("name", ""),
                    quantity=reagent_data.get("quantity", 1),
                )
            )

        crafted_item = data.get("crafted_item", {})
        crafted_quantity = data.get("crafted_quantity", {})

        return Recipe(
            id=data.get("id", 0),
            name=data.get("name", ""),
            crafted_item_id=crafted_item.get("id"),
            crafted_item_name=crafted_item.get("name"),
            crafted_quantity_min=crafted_quantity.get("minimum", 1),
            crafted_quantity_max=crafted_quantity.get("maximum", 1),
            reagents=tuple(reagents),
        )
