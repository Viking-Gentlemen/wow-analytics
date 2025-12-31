"""Use case for fetching and storing auction house data."""

from typing import Optional

import pandas as pd

from ports.auction_house import AuctionHousePort
from ports.storage import StoragePort


class FetchAndStoreAuctionsUseCase:
    """Orchestrates fetching auction data from API and storing it.

    This use case encapsulates the business logic for:
    - Fetching realm information
    - Fetching auction data
    - Persisting data to storage

    The use case is agnostic to the specific implementations of the
    auction house client and storage, relying on the port interfaces.

    Usage:
        use_case = FetchAndStoreAuctionsUseCase(client, writer)
        await use_case.execute()
    """

    def __init__(
        self,
        auction_house: AuctionHousePort,
        storage: StoragePort,
    ):
        """Initialize the use case with required ports.

        Args:
            auction_house: Implementation of the AuctionHousePort interface.
            storage: Implementation of the StoragePort interface.
        """
        self.auction_house = auction_house
        self.storage = storage

    async def fetch_and_store_realm_metadata(
        self,
        filename: str = "connected_realms.parquet",
    ) -> Optional[pd.DataFrame]:
        """Fetch all realm information and store it.

        Args:
            filename: Output filename for realm data.

        Returns:
            DataFrame of stored realm data or None if no realms.
        """
        print("\nFetching connected realms...")
        realm_ids = await self.auction_house.get_connected_realm_ids()
        print(f"Found {len(realm_ids)} connected realms")

        print("\nFetching realm details...")
        realm_details = await self.auction_house.get_all_realm_details(realm_ids)

        # Convert to list for storage
        realms = list(realm_details.values())

        return self.storage.save_connected_realms(realms, filename)

    async def fetch_and_store_realm_auctions(
        self,
        realm_ids: Optional[list[int]] = None,
        max_concurrent: int = 5,
        combined_filename: Optional[str] = None,
    ) -> dict[int, pd.DataFrame]:
        """Fetch auctions for specified realms and store them.

        Args:
            realm_ids: List of realm IDs to fetch. If None, fetches all.
            max_concurrent: Maximum concurrent API requests.
            combined_filename: If provided, saves all realms to a single file.
                              If None, saves each realm to a separate file.

        Returns:
            Dictionary mapping realm_id to DataFrame.
        """
        # Get all realm IDs if not specified
        if realm_ids is None:
            print("\nFetching connected realm IDs...")
            realm_ids = await self.auction_house.get_connected_realm_ids()
            print(f"Found {len(realm_ids)} connected realms")

        print(f"\nFetching auctions for {len(realm_ids)} realms...")
        auctions_by_realm = await self.auction_house.get_multiple_realm_auctions(
            realm_ids, max_concurrent=max_concurrent
        )

        if combined_filename:
            # Save all realms to a single file
            df = self.storage.save_combined_realm_auctions(
                auctions_by_realm, combined_filename
            )
            return {0: df} if df is not None else {}
        else:
            # Save each realm to a separate file
            return self.storage.save_multiple_realm_auctions(auctions_by_realm)

    async def fetch_and_store_commodities(
        self,
        filename: str = "commodities.parquet",
    ) -> Optional[pd.DataFrame]:
        """Fetch region-wide commodity auctions and store them.

        Args:
            filename: Output filename for commodity data.

        Returns:
            DataFrame of stored commodity data or None if no auctions.
        """
        print("\nFetching commodity auctions...")
        commodity_data = await self.auction_house.get_commodity_auctions()
        print(f"Found {len(commodity_data.auctions)} commodity auctions")

        return self.storage.save_auctions(commodity_data, filename)

    async def execute(
        self,
        include_realms: bool = True,
        include_auctions: bool = False,
        include_commodities: bool = False,
        combined_auction_file: Optional[str] = None,
    ) -> dict:
        """Execute the full fetch and store workflow.

        Args:
            include_realms: Whether to fetch and store realm metadata.
            include_auctions: Whether to fetch and store realm auctions.
            include_commodities: Whether to fetch and store commodity auctions.
            combined_auction_file: If provided, combines all auction data into one file.

        Returns:
            Dictionary with results for each component.
        """
        results = {}

        if include_realms:
            results["realms"] = await self.fetch_and_store_realm_metadata()

        if include_auctions:
            results["auctions"] = await self.fetch_and_store_realm_auctions(
                combined_filename=combined_auction_file
            )

        if include_commodities:
            results["commodities"] = await self.fetch_and_store_commodities()

        return results
