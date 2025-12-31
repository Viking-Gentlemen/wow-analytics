"""Blizzard API client for Recipe/Profession data."""

from typing import Any, Optional

from adapters.blizzard_api.base import BlizzardAPIClient
from domain.models import Profession, Recipe
from ports.recipes import RecipesPort


class RecipesClient(BlizzardAPIClient, RecipesPort):
    """Client for fetching World of Warcraft Recipe and Profession data from Battle.net API.

    Usage:
        async with RecipesClient(client_id, client_secret) as client:
            professions = await client.get_profession_index()
            recipe = await client.get_recipe(12345)
    """

    async def get_profession_index(self) -> list[Profession]:
        """Get index of all professions.

        Returns:
            List of Profession objects.
        """
        data = await self._get(
            "/data/wow/profession/index",
            namespace=self.namespace_static,
        )
        return [Profession.from_api_response(p) for p in data.get("professions", [])]

    async def get_profession(self, profession_id: int) -> dict[str, Any]:
        """Get details for a specific profession.

        Args:
            profession_id: The profession ID.

        Returns:
            Profession details including skill tiers.
        """
        return await self._get(
            f"/data/wow/profession/{profession_id}",
            namespace=self.namespace_static,
        )

    async def get_profession_media(self, profession_id: int) -> dict[str, Any]:
        """Get media assets for a profession.

        Args:
            profession_id: The profession ID.

        Returns:
            Media assets including icon URL.
        """
        return await self._get(
            f"/data/wow/media/profession/{profession_id}",
            namespace=self.namespace_static,
        )

    async def get_profession_skill_tier(self, profession_id: int, skill_tier_id: int) -> dict[str, Any]:
        """Get skill tier details for a profession (e.g., Dragonflight Alchemy).

        Args:
            profession_id: The profession ID.
            skill_tier_id: The skill tier ID.

        Returns:
            Skill tier details including recipe categories.
        """
        return await self._get(
            f"/data/wow/profession/{profession_id}/skill-tier/{skill_tier_id}",
            namespace=self.namespace_static,
        )

    async def get_recipe(self, recipe_id: int) -> Recipe:
        """Get recipe details by ID.

        Args:
            recipe_id: The recipe ID.

        Returns:
            Recipe object with full details including reagents.
        """
        data = await self._get(
            f"/data/wow/recipe/{recipe_id}",
            namespace=self.namespace_static,
        )
        return Recipe.from_api_response(data)

    async def get_recipe_media(self, recipe_id: int) -> dict[str, Any]:
        """Get media assets for a recipe.

        Args:
            recipe_id: The recipe ID.

        Returns:
            Media assets including icon URL.
        """
        return await self._get(
            f"/data/wow/media/recipe/{recipe_id}",
            namespace=self.namespace_static,
        )

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
        params = {
            "orderby": order_by,
            "_page": page,
        }
        if name:
            params["name.en_US"] = name

        return await self._get(
            "/data/wow/search/recipe",
            params=params,
            namespace=self.namespace_static,
        )
