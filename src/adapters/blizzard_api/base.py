"""Base Blizzard API client with OAuth authentication and common utilities."""

import time
from typing import Any, Optional, Self

from authlib.integrations.httpx_client import AsyncOAuth2Client


class BlizzardAPIClient:
    """Base client for Blizzard Battle.net API with OAuth2 authentication.

    This class provides the common functionality needed by all Blizzard API clients:
    - OAuth2 client credentials authentication
    - Token management and auto-refresh
    - Region and namespace configuration
    - HTTP client lifecycle management

    Subclasses should implement specific API endpoints.

    Usage:
        class MyClient(BlizzardAPIClient):
            async def get_something(self) -> dict:
                return await self._get("/data/wow/something")
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

        self._client: Optional[AsyncOAuth2Client] = None
        self._token: Optional[dict] = None

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        await self._authenticate()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        if self._client:
            await self._client.aclose()

    async def _authenticate(self) -> None:
        """Authenticate and get access token using client credentials flow."""
        try:
            self._client = AsyncOAuth2Client(
                client_id=self.client_id,
                client_secret=self.client_secret,
                token_endpoint=self.token_url,
            )

            self._token = await self._client.fetch_token(
                self.token_url,
                grant_type="client_credentials",
            )
            print(f"Successfully authenticated. Token expires in {self._token.get('expires_in')} seconds")
        except Exception as e:
            print(f"Authentication failed: {e}")
            raise

    async def _ensure_valid_token(self) -> None:
        """Check if token is valid and refresh if necessary."""
        if not self._token or self._token.get("expires_at", 0) <= time.time():
            print("Token expired or missing, re-authenticating...")
            await self._authenticate()

    async def _get(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        namespace: Optional[str] = None,
    ) -> dict[str, Any]:
        """Make an authenticated GET request to the API.

        Args:
            endpoint: API endpoint path (e.g., "/data/wow/item/19019").
            params: Additional query parameters.
            namespace: Override the default namespace (dynamic or static).

        Returns:
            JSON response as a dictionary.
        """
        await self._ensure_valid_token()

        url = f"{self.api_base}{endpoint}"
        request_params = {
            "namespace": namespace or self.namespace_dynamic,
            "locale": self.locale,
        }
        if params:
            request_params.update(params)

        response = await self._client.get(url, params=request_params)
        response.raise_for_status()

        return response.json()
