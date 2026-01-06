"""
Parquet Storage Port - Interface for writing data to Parquet format.

This port defines a generic contract for Parquet file storage.
It has NO knowledge of specific domain types (auctions, items, etc.).
The port works with:
- pandas DataFrames (the data to write)
- PyArrow schemas (optional explicit schema definition)
- Partition columns (for directory-based partitioning)

The use case layer is responsible for converting domain models to DataFrames
before calling this port.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa


class ParquetStoragePort(ABC):
    """
    Abstract interface for Parquet file storage.

    This is a generic storage port that:
    - Accepts DataFrames and writes them as Parquet files
    - Supports optional PyArrow schemas for type enforcement
    - Supports partitioning by columns
    - Has no knowledge of domain-specific data types

    Implementations can write to local filesystem, S3, GCS, etc.
    """

    @property
    @abstractmethod
    def base_path(self) -> str:
        """
        Return the base path/URI for storage.

        For local storage: absolute filesystem path
        For S3: s3://bucket/prefix
        For GCS: gs://bucket/prefix
        """
        pass

    @abstractmethod
    def write(
        self,
        df: pd.DataFrame,
        path: str,
        schema: Optional[pa.Schema] = None,
        partition_cols: Optional[list[str]] = None,
        compression: str = "snappy",
    ) -> str:
        """
        Write a DataFrame to a Parquet file or partitioned dataset.

        Args:
            df: The DataFrame to write.
            path: Relative path from base_path (e.g., "auctions/data.parquet").
            schema: Optional PyArrow schema. If provided, the DataFrame will be
                   converted to match this schema. If None, schema is inferred.
            partition_cols: Optional list of column names to partition by.
                          When provided, writes a partitioned dataset (directory
                          structure) instead of a single file.
            compression: Compression codec (snappy, gzip, zstd, none).

        Returns:
            The full path/URI where the data was written.

        Raises:
            StorageError: If the write operation fails.
        """
        pass

    @abstractmethod
    def write_dataset(
        self,
        df: pd.DataFrame,
        path: str,
        partition_cols: list[str],
        schema: Optional[pa.Schema] = None,
        compression: str = "snappy",
        existing_data_behavior: str = "overwrite_or_ignore",
    ) -> str:
        """
        Write a DataFrame as a partitioned Parquet dataset.

        This is optimized for partitioned writes with options for handling
        existing data.

        Args:
            df: The DataFrame to write.
            path: Relative path from base_path for the dataset root.
            partition_cols: Column names to partition by (required).
            schema: Optional PyArrow schema.
            compression: Compression codec.
            existing_data_behavior: How to handle existing partitions:
                - "overwrite_or_ignore": Overwrite existing files, ignore others
                - "error": Raise error if any partition exists
                - "delete_matching": Delete all data in matching partitions first

        Returns:
            The full path/URI where the dataset was written.
        """
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Check if a file or directory exists at the given path.

        Args:
            path: Relative path from base_path.

        Returns:
            True if the path exists, False otherwise.
        """
        pass

    @abstractmethod
    def list_files(self, path: str, pattern: str = "*.parquet") -> list[str]:
        """
        List files matching a pattern in a directory.

        Args:
            path: Relative path from base_path.
            pattern: Glob pattern to match files (default: "*.parquet").

        Returns:
            List of relative paths to matching files.
        """
        pass

    @abstractmethod
    def delete(self, path: str) -> bool:
        """
        Delete a file or directory.

        Args:
            path: Relative path from base_path.

        Returns:
            True if deletion was successful, False if path didn't exist.
        """
        pass

    def full_path(self, relative_path: str) -> str:
        """
        Convert a relative path to a full path/URI.

        Args:
            relative_path: Path relative to base_path.

        Returns:
            Full path/URI.
        """
        base = self.base_path.rstrip("/")
        rel = relative_path.lstrip("/")
        return f"{base}/{rel}"


class StorageError(Exception):
    """Exception raised for storage operation failures."""

    pass
