"""
Data transformers - Convert domain models to DataFrames for storage.

This module contains the logic for transforming domain models into pandas
DataFrames with appropriate schemas for Parquet storage. This transformation
belongs in the use case layer because:

1. The domain layer should remain pure (no pandas/pyarrow dependencies)
2. The storage port should be generic (no domain knowledge)
3. The use case orchestrates between domain and infrastructure

The schemas defined here are optimized for Parquet storage and querying.
"""

from datetime import datetime
from typing import Optional

import pandas as pd
import pyarrow as pa

from domain.models import AuctionData, ConnectedRealm


# =============================================================================
# PyArrow Schemas
# =============================================================================

AUCTION_SCHEMA = pa.schema(
    [
        ("auction_id", pa.int64()),
        ("item_id", pa.int32()),
        ("quantity", pa.int16()),
        ("time_left", pa.dictionary(pa.int8(), pa.string())),
        ("unit_price", pa.int64()),
        ("buyout", pa.int64()),
        ("bid", pa.int64()),
        ("bonus_lists", pa.string()),  # JSON-encoded list
        ("modifiers", pa.string()),  # JSON-encoded list of tuples
        ("connected_realm_id", pa.int32()),
        ("fetch_timestamp", pa.timestamp("us", tz="UTC")),
        # Partition columns
        ("date", pa.string()),
        ("hour", pa.string()),
    ]
)

CONNECTED_REALM_SCHEMA = pa.schema(
    [
        ("id", pa.int32()),
        ("realm_names", pa.string()),  # Comma-separated
        ("realm_slugs", pa.string()),  # Comma-separated
        ("status", pa.dictionary(pa.int8(), pa.string())),
        ("population", pa.dictionary(pa.int8(), pa.string())),
        ("has_queue", pa.bool_()),
    ]
)


# =============================================================================
# Transformer Functions
# =============================================================================


def auctions_to_dataframe(
    auction_data: AuctionData,
    include_partition_cols: bool = True,
) -> pd.DataFrame:
    """
    Convert AuctionData domain model to a DataFrame.

    Args:
        auction_data: The auction data to convert.
        include_partition_cols: Whether to include date/hour partition columns.

    Returns:
        DataFrame with flattened auction records.
    """
    records = []
    timestamp = auction_data.fetch_timestamp

    for auction in auction_data.auctions:
        record = {
            "auction_id": auction.id,
            "item_id": auction.item.id,
            "quantity": auction.quantity,
            "time_left": auction.time_left,
            "unit_price": auction.unit_price,
            "buyout": auction.buyout,
            "bid": auction.bid,
            "bonus_lists": str(list(auction.item.bonus_lists)) if auction.item.bonus_lists else "[]",
            "modifiers": str(list(auction.item.modifiers)) if auction.item.modifiers else "[]",
            "connected_realm_id": auction_data.connected_realm_id,
            "fetch_timestamp": timestamp,
        }

        if include_partition_cols:
            record["date"] = timestamp.strftime("%Y-%m-%d")
            record["hour"] = timestamp.strftime("%H")

        records.append(record)

    df = pd.DataFrame(records)

    # Optimize dtypes
    if len(df) > 0:
        df = _optimize_auction_dtypes(df)

    return df


def connected_realms_to_dataframe(realms: dict[int, ConnectedRealm]) -> pd.DataFrame:
    """
    Convert connected realms to a DataFrame.

    Args:
        realms: Dictionary mapping realm_id to ConnectedRealm.

    Returns:
        DataFrame with realm records.
    """
    records = []

    for realm_id, realm in realms.items():
        record = {
            "id": realm.id,
            "realm_names": ",".join(realm.realm_names),
            "realm_slugs": ",".join(realm.realm_slugs),
            "status": realm.status.value,
            "population": realm.population.value,
            "has_queue": realm.has_queue,
        }
        records.append(record)

    df = pd.DataFrame(records)

    if len(df) > 0:
        df = _optimize_realm_dtypes(df)

    return df


def _optimize_auction_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize DataFrame column dtypes for efficient Parquet storage.

    Uses:
    - int64 for auction_id (large values)
    - int32 for item_id and realm_id
    - int16 for quantity
    - nullable Int64 for optional price columns
    - category for time_left (small cardinality)
    """
    # Integer columns
    df["auction_id"] = df["auction_id"].astype("int64")
    df["item_id"] = df["item_id"].astype("int32")
    df["quantity"] = df["quantity"].astype("int16")
    df["connected_realm_id"] = df["connected_realm_id"].astype("int32")

    # Nullable integer columns (prices can be null)
    df["unit_price"] = df["unit_price"].astype("Int64")
    df["buyout"] = df["buyout"].astype("Int64")
    df["bid"] = df["bid"].astype("Int64")

    # Categorical for low-cardinality string columns
    df["time_left"] = df["time_left"].astype("category")

    return df


def _optimize_realm_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame column dtypes for realm data."""
    df["id"] = df["id"].astype("int32")
    df["status"] = df["status"].astype("category")
    df["population"] = df["population"].astype("category")
    df["has_queue"] = df["has_queue"].astype("bool")

    return df


def generate_auction_path(
    timestamp: datetime,
    realm_id: int,
    base_path: str = "auctions",
) -> str:
    """
    Generate a file path for auction data based on timestamp and realm.

    Args:
        timestamp: The fetch timestamp.
        realm_id: The connected realm ID (0 for commodities).
        base_path: Base directory for auction files.

    Returns:
        Path like "auctions/2024-01-15/14/realm_1234/auctions_20240115140532.parquet"
    """
    date_str = timestamp.strftime("%Y-%m-%d")
    hour_str = timestamp.strftime("%H")
    file_ts = timestamp.strftime("%Y%m%d%H%M%S")

    if realm_id == 0:
        return f"{base_path}/{date_str}/{hour_str}/commodities/commodities_{file_ts}.parquet"
    else:
        return f"{base_path}/{date_str}/{hour_str}/realm_{realm_id}/auctions_{file_ts}.parquet"
