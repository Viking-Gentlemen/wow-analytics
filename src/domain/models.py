"""Domain models representing core business entities."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class RealmStatus(str, Enum):
    """Realm status types."""

    UP = "UP"
    DOWN = "DOWN"


class PopulationType(str, Enum):
    """Realm population types."""

    FULL = "FULL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    NEW_PLAYERS = "NEW_PLAYERS"
    RECOMMENDED = "RECOMMENDED"


@dataclass(frozen=True)
class AuctionItem:
    """Represents an item in an auction."""

    id: int
    bonus_lists: Optional[tuple[int, ...]] = None
    modifiers: Optional[dict[int, int]] = None


@dataclass(frozen=True)
class Auction:
    """Represents a single auction listing."""

    id: int
    item: AuctionItem
    quantity: int
    time_left: str
    unit_price: Optional[int] = None
    buyout: Optional[int] = None
    bid: Optional[int] = None


@dataclass
class AuctionData:
    """Collection of auctions with metadata."""

    connected_realm_id: Optional[int]
    auctions: list[Auction]
    fetch_timestamp: datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def from_api_response(cls, response: dict, connected_realm_id: Optional[int] = None) -> "AuctionData":
        """Create AuctionData from raw API response."""
        auctions = []
        for auction_data in response.get("auctions", []):
            item_data = auction_data.get("item", {})
            item = AuctionItem(
                id=item_data.get("id"),
                bonus_lists=tuple(item_data.get("bonus_lists", [])) if "bonus_lists" in item_data else None,
                modifiers={mod.get("type"): mod.get("value") for mod in item_data.get("modifiers", [])} if "modifiers" in item_data else None,
            )
            auction = Auction(
                id=auction_data.get("id"),
                item=item,
                quantity=auction_data.get("quantity", 1),
                time_left=auction_data.get("time_left"),
                unit_price=auction_data.get("unit_price"),
                buyout=auction_data.get("buyout"),
                bid=auction_data.get("bid"),
            )
            auctions.append(auction)

        return cls(
            connected_realm_id=connected_realm_id,
            auctions=auctions,
        )


@dataclass(frozen=True)
class ConnectedRealm:
    """Represents a connected realm (group of merged realms)."""

    id: int
    realm_names: tuple[str, ...] = field(default_factory=tuple)
    realm_slugs: tuple[str, ...] = field(default_factory=tuple)
    status: Optional[RealmStatus] = None
    population: Optional[PopulationType] = None
    has_queue: bool = False

    @classmethod
    def from_api_response(cls, realm_id: int, response: dict) -> "ConnectedRealm":
        """Create ConnectedRealm from raw API response."""
        realms = response.get("realms", [])
        return cls(
            id=realm_id,
            realm_names=tuple(r.get("name", "") for r in realms),
            realm_slugs=tuple(r.get("slug", "") for r in realms),
            status=RealmStatus(response.get("status", {}).get("type")) if response.get("status", {}).get("type") else None,
            population=PopulationType(response.get("population", {}).get("type")) if response.get("population", {}).get("type") else None,
            has_queue=response.get("has_queue", False),
        )


@dataclass
class RealmDetails:
    """Collection of realm details."""

    realms: dict[int, ConnectedRealm] = field(default_factory=dict)

    def add(self, realm: ConnectedRealm) -> None:
        """Add a realm to the collection."""
        self.realms[realm.id] = realm

    def get(self, realm_id: int) -> Optional[ConnectedRealm]:
        """Get a realm by ID."""
        return self.realms.get(realm_id)

    def __iter__(self):
        """Iterate over realms."""
        return iter(self.realms.values())

    def __len__(self):
        """Return the number of realms."""
        return len(self.realms)


# Item models


@dataclass
class Item:
    """Represents a World of Warcraft item."""

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
    description: Optional[str] = None

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "Item":
        """Create an Item from API response data."""
        return cls(
            id=data["id"],
            name=data.get("name", "Unknown"),
            quality=data.get("quality", {}).get("type", "COMMON"),
            level=data.get("level", 0),
            item_class=data.get("item_class", {}).get("name", "Unknown"),
            item_subclass=data.get("item_subclass", {}).get("name", "Unknown"),
            inventory_type=data.get("inventory_type", {}).get("type", "NON_EQUIP"),
            purchase_price=data.get("purchase_price", 0),
            sell_price=data.get("sell_price", 0),
            max_count=data.get("max_count", 1),
            is_equippable=data.get("is_equippable", False),
            is_stackable=data.get("is_stackable", False),
            description=data.get("description"),
        )


@dataclass
class ItemMedia:
    """Represents media assets for an item."""

    item_id: int
    icon_url: Optional[str] = None

    @classmethod
    def from_api_response(cls, item_id: int, data: dict[str, Any]) -> "ItemMedia":
        """Create ItemMedia from API response data."""
        assets = data.get("assets", [])
        icon_url = None
        for asset in assets:
            if asset.get("key") == "icon":
                icon_url = asset.get("value")
                break

        return cls(item_id=item_id, icon_url=icon_url)


# Recipe and Profession models


@dataclass
class RecipeReagent:
    """Represents a reagent required for a recipe."""

    item_id: int
    item_name: str
    quantity: int

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "RecipeReagent":
        """Create a RecipeReagent from API response data."""
        reagent = data.get("reagent", {})
        return cls(
            item_id=reagent.get("id", 0),
            item_name=reagent.get("name", "Unknown"),
            quantity=data.get("quantity", 1),
        )


@dataclass
class Recipe:
    """Represents a World of Warcraft crafting recipe."""

    id: int
    name: str
    crafted_item_id: Optional[int]
    crafted_item_name: Optional[str]
    crafted_quantity_min: int
    crafted_quantity_max: int
    reagents: list[RecipeReagent] = field(default_factory=list)

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "Recipe":
        """Create a Recipe from API response data."""
        crafted_item = data.get("crafted_item", {})
        crafted_quantity = data.get("crafted_quantity", {})

        reagents = [RecipeReagent.from_api_response(r) for r in data.get("reagents", [])]

        return cls(
            id=data["id"],
            name=data.get("name", "Unknown"),
            crafted_item_id=crafted_item.get("id"),
            crafted_item_name=crafted_item.get("name"),
            crafted_quantity_min=crafted_quantity.get("minimum", 1),
            crafted_quantity_max=crafted_quantity.get("maximum", 1),
            reagents=reagents,
        )


@dataclass
class Profession:
    """Represents a World of Warcraft profession."""

    id: int
    name: str
    type: str  # PRIMARY or SECONDARY

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "Profession":
        """Create a Profession from API response data."""
        return cls(
            id=data["id"],
            name=data.get("name", "Unknown"),
            type=data.get("type", {}).get("type", "PRIMARY"),
        )
