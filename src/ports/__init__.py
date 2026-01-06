"""Ports layer - Abstract interfaces defining contracts with the outside world."""

from ports.blizzard_api import BlizzardAPIPort
from ports.parquet_storage import ParquetStoragePort

__all__ = [
    "BlizzardAPIPort",
    "ParquetStoragePort",
]
