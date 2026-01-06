"""
Domain models - Pure business entities.

These are immutable value objects representing the core business concepts.
They contain NO knowledge of external systems (APIs, databases, file formats).
Transformation logic from external formats belongs in the adapter layer.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


# =============================================================================
# Realm Domain Models
# =============================================================================


class RealmStatus(Enum):
    """Realm operational status."""

    UP = "UP"
    DOWN = "DOWN"


class PopulationType(Enum):
    """Realm population classification."""

    FULL = "FULL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    NEW_PLAYERS = "NEW_PLAYERS"
    RECOMMENDED = "RECOMMENDED"


@dataclass(frozen=True)
class ConnectedRealm:
    """
    A connected realm grouping in World of Warcraft.

    Connected realms share an auction house and can group together for content.
    """

    id: int
    realm_names: tuple[str, ...]
    realm_slugs: tuple[str, ...]
    status: RealmStatus
    population: PopulationType
    has_queue: bool


@dataclass
class RealmDetails:
    """Container for managing a collection of connected realms."""

    _realms: dict[int, ConnectedRealm] = field(default_factory=dict)

    def add(self, realm: ConnectedRealm) -> None:
        """Add a realm to the collection."""
        self._realms[realm.id] = realm

    def get(self, realm_id: int) -> Optional[ConnectedRealm]:
        """Retrieve a realm by ID."""
        return self._realms.get(realm_id)

    def __iter__(self):
        """Iterate over all realms."""
        return iter(self._realms.values())

    def __len__(self) -> int:
        """Return the number of realms."""
        return len(self._realms)


# =============================================================================
# Auction Domain Models
# =============================================================================


@dataclass(frozen=True)
class AuctionItem:
    """
    An item listed in an auction.

    Contains item identity and optional modifiers that affect the item.
    """

    id: int
    bonus_lists: tuple[int, ...] = field(default_factory=tuple)
    modifiers: tuple[tuple[int, int], ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class Auction:
    """
    A single auction listing.

    Represents an item for sale on the auction house with pricing information.
    """

    id: int
    item: AuctionItem
    quantity: int
    time_left: str
    unit_price: Optional[int] = None
    buyout: Optional[int] = None
    bid: Optional[int] = None


@dataclass
class AuctionData:
    """
    Collection of auctions from a specific realm at a point in time.

    Contains all auction listings fetched from a connected realm.
    """

    connected_realm_id: int
    auctions: list[Auction]
    fetch_timestamp: datetime


# =============================================================================
# Item Domain Models
# =============================================================================


@dataclass(frozen=True)
class Item:
    """
    A World of Warcraft item.

    Contains item metadata and economic information.
    """

    id: int
    name: str
    quality: str
    level: int
    item_class: str
    item_subclass: str
    inventory_type: str
    purchase_price: int
    sell_price: int
    max_count: int
    is_equippable: bool
    is_stackable: bool
    description: str = ""


@dataclass(frozen=True)
class ItemMedia:
    """Media assets for an item (icon, etc.)."""

    item_id: int
    icon_url: str


# =============================================================================
# Recipe/Profession Domain Models
# =============================================================================


@dataclass(frozen=True)
class RecipeReagent:
    """A reagent required for crafting a recipe."""

    item_id: int
    item_name: str
    quantity: int


@dataclass(frozen=True)
class Recipe:
    """
    A crafting recipe.

    Contains information about what is crafted and the required materials.
    """

    id: int
    name: str
    crafted_item_id: Optional[int]
    crafted_item_name: Optional[str]
    crafted_quantity_min: int
    crafted_quantity_max: int
    reagents: tuple[RecipeReagent, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class Profession:
    """
    A crafting or gathering profession.

    PRIMARY professions (2 max): Alchemy, Blacksmithing, etc.
    SECONDARY professions: Cooking, Fishing, etc.
    """

    id: int
    name: str
    type: str  # "PRIMARY" or "SECONDARY"
