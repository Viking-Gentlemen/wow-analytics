"""Parquet storage adapter for persisting auction house data."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from domain.models import AuctionData, ConnectedRealm
from ports.storage import StoragePort


class ParquetWriter(StoragePort):
    """Handles writing Battle.net API data to Parquet format.

    This adapter implements the StoragePort interface, providing persistence
    of auction house data to Parquet files on the local filesystem.

    Usage:
        writer = ParquetWriter(root_dir="/data", region="eu")
        writer.save_auctions(auction_data, "auctions.parquet")
    """

    def __init__(
        self,
        root_dir: str | Path,
        region: str,
        compression: str = "snappy",
    ):
        """Initialize the Parquet writer.

        Args:
            root_dir: Directory to save Parquet files.
            region: Name of the Blizzard region.
            compression: Compression algorithm ('snappy', 'gzip', 'brotli', 'zstd').

        Raises:
            FileNotFoundError: If the root_dir does not exist.
            NotADirectoryError: If the root_dir is not a directory.
            PermissionError: If the root_dir is not writable.
        """
        self.region = region
        self.root_dir = Path(root_dir)

        # Validate root_dir exists
        if not self.root_dir.exists():
            raise FileNotFoundError(f"Root data directory does not exist: {self.root_dir}")

        # Validate root_dir is a directory
        if not self.root_dir.is_dir():
            raise NotADirectoryError(f"Root data path is not a directory: {self.root_dir}")

        # Validate root_dir is writable
        if not os.access(self.root_dir, os.W_OK):
            raise PermissionError(f"Root data directory is not writable: {self.root_dir}")

        self._data_dir = self.root_dir / self.region
        self._data_dir.mkdir(mode=0o774, exist_ok=True)

        self.compression = compression

    @property
    def data_dir(self) -> Path:
        """Get the data directory path."""
        return self._data_dir

    def save_auctions(
        self,
        auction_data: AuctionData,
        filename: str,
    ) -> Optional[pd.DataFrame]:
        """Save auction data to Parquet format with optimized schema.

        Args:
            auction_data: The auction data to save.
            filename: Output filename.

        Returns:
            DataFrame of the saved data or None if no auctions.
        """
        if not auction_data.auctions:
            print("No auctions to save")
            return None

        # Flatten the auction data
        flattened_auctions = []

        for auction in auction_data.auctions:
            flat_auction = {
                "fetch_timestamp": auction_data.fetch_timestamp,
                "auction_id": auction.id,
                "item_id": auction.item.id,
                "quantity": auction.quantity,
                "unit_price": auction.unit_price,
                "buyout": auction.buyout,
                "time_left": auction.time_left,
            }

            # Add optional fields
            if auction_data.connected_realm_id:
                flat_auction["connected_realm_id"] = auction_data.connected_realm_id

            # Handle item bonuses
            if auction.item.bonus_lists:
                flat_auction["bonus_lists"] = ",".join(map(str, auction.item.bonus_lists))

            if auction.item.modifiers:
                for mod_type, mod_value in auction.item.modifiers.items():
                    flat_auction[f"modifier_{mod_type}"] = mod_value

            # Handle bid
            if auction.bid:
                flat_auction["bid"] = auction.bid

            flattened_auctions.append(flat_auction)

        # Convert to DataFrame
        df = pd.DataFrame(flattened_auctions)

        # Optimize data types for better compression
        df = self._optimize_auction_dtypes(df)

        # Save
        filepath = self._resolve_filepath(filename)
        df.to_parquet(
            filepath,
            engine="pyarrow",
            compression=self.compression,
            index=False,
        )

        file_size = filepath.stat().st_size / (1024 * 1024)  # Size in MB
        print(f"Saved {len(df)} auctions to {filepath} ({file_size:.2f} MB)")

        return df

    def save_connected_realms(
        self,
        realms: list[ConnectedRealm],
        filename: str,
    ) -> Optional[pd.DataFrame]:
        """Save connected realms data to Parquet format.

        Args:
            realms: List of ConnectedRealm objects.
            filename: Output filename.

        Returns:
            DataFrame of the saved data or None if no realms.
        """
        if not realms:
            print("No connected realms to save")
            return None

        # Convert realms to flat dictionaries
        realm_list = []
        for realm in realms:
            realm_info = {
                "connected_realm_id": realm.id,
                "has_queue": realm.has_queue,
                "status_type": realm.status.value if realm.status else None,
                "population_type": realm.population.value if realm.population else None,
                "num_realms": len(realm.realm_names),
                "realm_names": ", ".join(realm.realm_names),
                "realm_slugs": ", ".join(realm.realm_slugs),
            }
            realm_list.append(realm_info)

        df = pd.DataFrame(realm_list)

        # Optimize data types
        df = self._optimize_realm_dtypes(df)

        # Save
        filepath = self._resolve_filepath(f"global/{filename}")
        df.to_parquet(
            filepath,
            engine="pyarrow",
            compression=self.compression,
            index=False,
        )
        print(f"Saved {len(df)} connected realms to {filepath}")

        return df

    def save_multiple_realm_auctions(
        self,
        auctions_by_realm: dict[int, AuctionData],
        timestamp_str: Optional[str] = None,
    ) -> dict[int, pd.DataFrame]:
        """Save auction data for multiple realms to separate Parquet files.

        Args:
            auctions_by_realm: Dictionary mapping realm_id to AuctionData.
            timestamp_str: Optional timestamp string for filenames.

        Returns:
            Dictionary mapping realm_id to DataFrame.
        """
        if timestamp_str is None:
            timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        dataframes = {}
        for realm_id, auction_data in auctions_by_realm.items():
            filename = f"auctions/{realm_id}/auctions_realm_{realm_id}_{timestamp_str}.parquet"
            df = self.save_auctions(auction_data, filename)
            if df is not None:
                dataframes[realm_id] = df

        return dataframes

    def save_combined_realm_auctions(
        self,
        auctions_by_realm: dict[int, AuctionData],
        filename: str,
    ) -> Optional[pd.DataFrame]:
        """Save auction data for multiple realms to a single Parquet file.

        Args:
            auctions_by_realm: Dictionary mapping realm_id to AuctionData.
            filename: Output filename.

        Returns:
            Combined DataFrame or None if no auctions.
        """
        all_dfs = []

        for realm_id, auction_data in auctions_by_realm.items():
            if not auction_data.auctions:
                continue

            # Flatten auctions for this realm
            flattened = []
            for auction in auction_data.auctions:
                flat_auction = {
                    "fetch_timestamp": auction_data.fetch_timestamp,
                    "connected_realm_id": realm_id,
                    "auction_id": auction.id,
                    "item_id": auction.item.id,
                    "quantity": auction.quantity,
                    "unit_price": auction.unit_price,
                    "buyout": auction.buyout,
                    "time_left": auction.time_left,
                }

                # Handle item bonuses
                if auction.item.bonus_lists:
                    flat_auction["bonus_lists"] = ",".join(map(str, auction.item.bonus_lists))

                if auction.bid:
                    flat_auction["bid"] = auction.bid

                flattened.append(flat_auction)

            df = pd.DataFrame(flattened)
            all_dfs.append(df)

        if not all_dfs:
            print("No auctions to save")
            return None

        # Combine all dataframes
        combined_df = pd.concat(all_dfs, ignore_index=True)
        combined_df = self._optimize_auction_dtypes(combined_df)

        # Save
        filepath = self._resolve_filepath(filename)
        combined_df.to_parquet(
            filepath,
            engine="pyarrow",
            compression=self.compression,
            index=False,
        )

        file_size = filepath.stat().st_size / (1024 * 1024)
        print(f"Saved {len(combined_df)} total auctions from {len(auctions_by_realm)} realms " f"to {filepath} ({file_size:.2f} MB)")

        return combined_df

    def save_to_json(self, data: dict, filename: str) -> None:
        """Save data to JSON file (for reference/debugging).

        Args:
            data: Data to save.
            filename: Output filename.
        """
        filepath = self._resolve_filepath(filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        print(f"Data saved to {filepath}")

    def _resolve_filepath(self, filename: str) -> Path:
        """Resolve filename to full filepath."""
        if Path(filename).is_absolute():
            filepath = Path(filename)
        else:
            filepath = self._data_dir / filename

        # Make sure parent directory exists
        filepath.parent.mkdir(parents=True, exist_ok=True)

        return filepath

    def _optimize_auction_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize data types for auction DataFrames."""
        if "auction_id" in df.columns:
            df["auction_id"] = df["auction_id"].astype("int64")
        if "item_id" in df.columns:
            df["item_id"] = df["item_id"].astype("int32")
        if "quantity" in df.columns:
            df["quantity"] = df["quantity"].astype("int16")
        if "unit_price" in df.columns:
            df["unit_price"] = df["unit_price"].astype("Int64")  # Nullable int
        if "buyout" in df.columns:
            df["buyout"] = df["buyout"].astype("Int64")  # Nullable int
        if "bid" in df.columns:
            df["bid"] = df["bid"].astype("Int64")  # Nullable int
        if "time_left" in df.columns:
            df["time_left"] = df["time_left"].astype("category")
        if "connected_realm_id" in df.columns:
            df["connected_realm_id"] = df["connected_realm_id"].astype("int16")
        return df

    def _optimize_realm_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize data types for realm DataFrames."""
        if "connected_realm_id" in df.columns:
            df["connected_realm_id"] = df["connected_realm_id"].astype("int16")
        if "has_queue" in df.columns:
            df["has_queue"] = df["has_queue"].astype("bool")
        if "status_type" in df.columns:
            df["status_type"] = df["status_type"].astype("category")
        if "population_type" in df.columns:
            df["population_type"] = df["population_type"].astype("category")
        if "num_realms" in df.columns:
            df["num_realms"] = df["num_realms"].astype("int8")
        return df
