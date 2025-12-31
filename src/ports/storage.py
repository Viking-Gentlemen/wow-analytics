"""Port (interface) for data storage."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import pandas as pd

from domain.models import AuctionData, ConnectedRealm


class StoragePort(ABC):
    """Abstract interface for data storage.

    This port defines the contract for persisting auction house data
    to any storage backend (Parquet files, database, cloud storage, etc.).
    """

    @abstractmethod
    def save_auctions(
        self,
        auction_data: AuctionData,
        filename: str,
    ) -> Optional[pd.DataFrame]:
        """Save auction data to storage.

        Args:
            auction_data: The auction data to save.
            filename: Output filename.

        Returns:
            DataFrame of the saved data or None if no auctions.
        """
        ...

    @abstractmethod
    def save_connected_realms(
        self,
        realms: list[ConnectedRealm],
        filename: str,
    ) -> Optional[pd.DataFrame]:
        """Save connected realms data to storage.

        Args:
            realms: List of ConnectedRealm objects.
            filename: Output filename.

        Returns:
            DataFrame of the saved data or None if no realms.
        """
        ...

    @abstractmethod
    def save_multiple_realm_auctions(
        self,
        auctions_by_realm: dict[int, AuctionData],
        timestamp_str: Optional[str] = None,
    ) -> dict[int, pd.DataFrame]:
        """Save auction data for multiple realms to separate files.

        Args:
            auctions_by_realm: Dictionary mapping realm_id to AuctionData.
            timestamp_str: Optional timestamp string for filenames.

        Returns:
            Dictionary mapping realm_id to DataFrame.
        """
        ...

    @abstractmethod
    def save_combined_realm_auctions(
        self,
        auctions_by_realm: dict[int, AuctionData],
        filename: str,
    ) -> Optional[pd.DataFrame]:
        """Save auction data for multiple realms to a single file.

        Args:
            auctions_by_realm: Dictionary mapping realm_id to AuctionData.
            filename: Output filename.

        Returns:
            Combined DataFrame or None if no auctions.
        """
        ...

    @property
    @abstractmethod
    def data_dir(self) -> Path:
        """Get the data directory path."""
        ...
