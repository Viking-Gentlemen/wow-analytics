"""Port (interface) for recipe and profession data retrieval."""

from abc import ABC, abstractmethod
from typing import Any, Optional

from domain.models import Profession, Recipe


class RecipesPort(ABC):
    """Abstract interface for recipe and profession data retrieval.

    This port defines the contract for fetching recipe and profession data
    from any source (Blizzard API, mock data, cache, etc.).
    """

    @abstractmethod
    async def __aenter__(self) -> "RecipesPort":
        """Async context manager entry."""
        ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        ...

    @abstractmethod
    async def get_profession_index(self) -> list[Profession]:
        """Get index of all professions.

        Returns:
            List of Profession objects.
        """
        ...

    @abstractmethod
    async def get_profession(self, profession_id: int) -> dict[str, Any]:
        """Get details for a specific profession.

        Args:
            profession_id: The profession ID.

        Returns:
            Profession details including skill tiers.
        """
        ...

    @abstractmethod
    async def get_profession_media(self, profession_id: int) -> dict[str, Any]:
        """Get media assets for a profession.

        Args:
            profession_id: The profession ID.

        Returns:
            Media assets including icon URL.
        """
        ...

    @abstractmethod
    async def get_profession_skill_tier(self, profession_id: int, skill_tier_id: int) -> dict[str, Any]:
        """Get skill tier details for a profession (e.g., Dragonflight Alchemy).

        Args:
            profession_id: The profession ID.
            skill_tier_id: The skill tier ID.

        Returns:
            Skill tier details including recipe categories.
        """
        ...

    @abstractmethod
    async def get_recipe(self, recipe_id: int) -> Recipe:
        """Get recipe details by ID.

        Args:
            recipe_id: The recipe ID.

        Returns:
            Recipe object with full details including reagents.
        """
        ...

    @abstractmethod
    async def get_recipe_media(self, recipe_id: int) -> dict[str, Any]:
        """Get media assets for a recipe.

        Args:
            recipe_id: The recipe ID.

        Returns:
            Media assets including icon URL.
        """
        ...

    @abstractmethod
    async def search_recipes(
        self,
        name: Optional[str] = None,
        order_by: str = "id",
        page: int = 1,
    ) -> dict[str, Any]:
        """Search for recipes.

        Args:
            name: Recipe name to search for (supports wildcards).
            order_by: Field to order results by.
            page: Page number for pagination.

        Returns:
            Search results with recipe references.
        """
        ...
