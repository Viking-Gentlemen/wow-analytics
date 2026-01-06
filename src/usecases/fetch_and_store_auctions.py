"""
Fetch and Store Auctions Use Case.

This use case orchestrates:
1. Fetching data from the Blizzard API (via BlizzardAPIPort)
2. Transforming domain models to DataFrames (via data_transformers)
3. Storing data as Parquet files (via ParquetStoragePort)

The use case has no knowledge of:
- How the API client authenticates or makes requests
- How the storage adapter writes files (local vs S3)

It only knows about:
- Domain models (what data looks like)
- Ports (interfaces for external operations)
- Transformers (how to convert domain to DataFrames)
"""

from typing import Optional

from domain.models import AuctionData, ConnectedRealm
from ports.blizzard_api import BlizzardAPIPort
from ports.parquet_storage import ParquetStoragePort
from usecases.data_transformers import (
    auctions_to_dataframe,
    connected_realms_to_dataframe,
    generate_auction_path,
    AUCTION_SCHEMA,
    CONNECTED_REALM_SCHEMA,
)


class FetchAndStoreAuctionsUseCase:
    """
    Use case for fetching auction data from Blizzard API and storing as Parquet.

    Follows hexagonal architecture:
    - Depends only on ports (interfaces), not adapters (implementations)
    - Orchestrates the workflow without knowing implementation details
    """

    def __init__(
        self,
        api: BlizzardAPIPort,
        storage: ParquetStoragePort,
    ):
        """
        Initialize the use case with required ports.

        Args:
            api: Blizzard API port implementation.
            storage: Parquet storage port implementation.
        """
        self._api = api
        self._storage = storage

    async def fetch_and_store_realm_metadata(
        self,
        filename: str = "global/connected_realms.parquet",
    ) -> dict[int, ConnectedRealm]:
        """
        Fetch all connected realm metadata and store as Parquet.

        Args:
            filename: Path for the output file (relative to storage base).

        Returns:
            Dictionary of realm_id -> ConnectedRealm.
        """
        # Fetch realm IDs
        realm_ids = await self._api.get_connected_realm_ids()

        # Fetch all realm details
        realms = await self._api.get_all_connected_realms(realm_ids)

        # Transform to DataFrame
        df = connected_realms_to_dataframe(realms)

        # Store as Parquet
        if len(df) > 0:
            self._storage.write(
                df=df,
                path=filename,
                schema=CONNECTED_REALM_SCHEMA,
            )

        return realms

    async def fetch_and_store_realm_auctions(
        self,
        realm_ids: Optional[list[int]] = None,
        max_concurrent: int = 5,
    ) -> dict[int, str]:
        """
        Fetch auctions for specified realms and store each as a separate Parquet file.

        Args:
            realm_ids: List of realm IDs to fetch. If None, fetches all realms.
            max_concurrent: Maximum concurrent API requests.

        Returns:
            Dictionary mapping realm_id to the output file path.
        """
        # Get realm IDs if not provided
        if realm_ids is None:
            realm_ids = await self._api.get_connected_realm_ids()

        # Fetch auctions for all realms
        auctions_by_realm = await self._api.get_multiple_realm_auctions(realm_ids, max_concurrent)

        # Store each realm's auctions separately
        result_paths = {}
        for realm_id, auction_data in auctions_by_realm.items():
            if len(auction_data.auctions) > 0:
                # Transform to DataFrame
                df = auctions_to_dataframe(auction_data)

                # Generate path
                path = generate_auction_path(
                    auction_data.fetch_timestamp,
                    realm_id,
                )

                # Store as Parquet
                full_path = self._storage.write(
                    df=df,
                    path=path,
                    schema=AUCTION_SCHEMA,
                )
                result_paths[realm_id] = full_path

        return result_paths

    async def fetch_and_store_commodities(
        self,
    ) -> Optional[str]:
        """
        Fetch region-wide commodity auctions and store as Parquet.

        Returns:
            Output file path, or None if no commodities.
        """
        # Fetch commodity auctions
        auction_data = await self._api.get_commodity_auctions()

        if len(auction_data.auctions) == 0:
            return None

        # Transform to DataFrame
        df = auctions_to_dataframe(auction_data)

        # Generate path (realm_id=0 for commodities)
        path = generate_auction_path(
            auction_data.fetch_timestamp,
            realm_id=0,
        )

        # Store as Parquet
        return self._storage.write(
            df=df,
            path=path,
            schema=AUCTION_SCHEMA,
        )

    async def execute(
        self,
        include_realms: bool = True,
        include_auctions: bool = True,
        include_commodities: bool = True,
        realm_ids: Optional[list[int]] = None,
        max_concurrent: int = 5,
    ) -> dict:
        """
        Execute the full auction data pipeline.

        Args:
            include_realms: Whether to fetch and store realm metadata.
            include_auctions: Whether to fetch and store realm auctions.
            include_commodities: Whether to fetch and store commodity auctions.
            realm_ids: Specific realm IDs to fetch auctions for.
            max_concurrent: Maximum concurrent API requests.

        Returns:
            Dictionary with results for each component:
            {
                "realms": dict[int, ConnectedRealm] or None,
                "auctions": dict[int, str] or None,  # realm_id -> file path
                "commodities": str or None,  # file path
            }
        """
        results = {
            "realms": None,
            "auctions": None,
            "commodities": None,
        }

        # Fetch and store realm metadata
        if include_realms:
            results["realms"] = await self.fetch_and_store_realm_metadata()

        # Fetch and store auctions
        if include_auctions:
            results["auctions"] = await self.fetch_and_store_realm_auctions(
                realm_ids=realm_ids,
                max_concurrent=max_concurrent,
            )

        # Fetch and store commodities
        if include_commodities:
            results["commodities"] = await self.fetch_and_store_commodities()

        return results
