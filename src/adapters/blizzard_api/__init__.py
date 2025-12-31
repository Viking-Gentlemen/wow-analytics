"""Blizzard API adapters for World of Warcraft data."""

from adapters.blizzard_api.auctions import AuctionsClient
from adapters.blizzard_api.base import BlizzardAPIClient
from adapters.blizzard_api.items import ItemsClient
from adapters.blizzard_api.recipes import RecipesClient

# Backwards compatibility alias
BlizzardAuctionHouseClient = AuctionsClient

__all__ = [
    # Base client
    "BlizzardAPIClient",
    # Specialized clients
    "AuctionsClient",
    "ItemsClient",
    "RecipesClient",
    # Backwards compatibility
    "BlizzardAuctionHouseClient",
]
