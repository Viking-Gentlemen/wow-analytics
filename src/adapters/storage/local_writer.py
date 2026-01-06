"""
Local Parquet Writer - Implementation of ParquetStoragePort for local filesystem.

This adapter writes Parquet files to the local filesystem.
It has NO knowledge of domain-specific data types - it works purely with
DataFrames and PyArrow schemas.
"""

import glob as glob_module
import shutil
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ports.parquet_storage import ParquetStoragePort, StorageError


class LocalParquetWriter(ParquetStoragePort):
    """
    Local filesystem Parquet storage implementation.

    Writes Parquet files to a local directory structure.
    Supports single file writes and partitioned datasets.
    """

    def __init__(
        self,
        root_dir: str | Path,
        default_compression: str = "snappy",
        region: str | None = None,
    ):
        """
        Initialize the local Parquet writer.

        Args:
            root_dir: Root directory for all Parquet files.
            default_compression: Default compression codec (snappy, gzip, zstd, none).
            region: Optional region name to use as root directory prefix.

        Raises:
            StorageError: If the directory doesn't exist or isn't writable.
        """
        base_dir = Path(root_dir).resolve()
        if region:
            self._root_dir = base_dir / region
        else:
            self._root_dir = base_dir
        self._default_compression = default_compression

        self._validate_directory()

    def _validate_directory(self) -> None:
        """Validate that the root directory exists and is writable."""
        if not self._root_dir.exists():
            try:
                self._root_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise StorageError(f"Cannot create directory {self._root_dir}: {e}")

        if not self._root_dir.is_dir():
            raise StorageError(f"Path is not a directory: {self._root_dir}")

        # Test write permission
        test_file = self._root_dir / ".write_test"
        try:
            test_file.touch()
            test_file.unlink()
        except OSError as e:
            raise StorageError(f"Directory is not writable: {self._root_dir}: {e}")

    @property
    def base_path(self) -> str:
        """Return the root directory as a string."""
        return str(self._root_dir)

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
            path: Relative path from base_path.
            schema: Optional PyArrow schema.
            partition_cols: Optional list of partition columns.
            compression: Compression codec.

        Returns:
            Full path where data was written.

        Raises:
            StorageError: If write fails.
        """
        if partition_cols:
            return self.write_dataset(
                df=df,
                path=path,
                partition_cols=partition_cols,
                schema=schema,
                compression=compression,
            )

        full_path = self._root_dir / path
        compression = compression or self._default_compression

        try:
            # Ensure parent directory exists
            full_path.parent.mkdir(parents=True, exist_ok=True)

            # Convert DataFrame to PyArrow Table
            if schema:
                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            else:
                table = pa.Table.from_pandas(df, preserve_index=False)

            # Write Parquet file
            pq.write_table(
                table,
                full_path,
                compression=compression if compression != "none" else None,
            )

            return str(full_path)

        except Exception as e:
            raise StorageError(f"Failed to write Parquet file {full_path}: {e}")

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

        Args:
            df: The DataFrame to write.
            path: Relative path for dataset root.
            partition_cols: Column names to partition by.
            schema: Optional PyArrow schema.
            compression: Compression codec.
            existing_data_behavior: How to handle existing partitions.

        Returns:
            Full path where dataset was written.

        Raises:
            StorageError: If write fails.
        """
        full_path = self._root_dir / path
        compression = compression or self._default_compression

        try:
            # Ensure directory exists
            full_path.mkdir(parents=True, exist_ok=True)

            # Convert DataFrame to PyArrow Table
            if schema:
                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            else:
                table = pa.Table.from_pandas(df, preserve_index=False)

            # Write partitioned dataset
            pq.write_to_dataset(
                table,
                root_path=str(full_path),
                partition_cols=partition_cols,
                compression=compression if compression != "none" else None,
                existing_data_behavior=existing_data_behavior,
            )

            return str(full_path)

        except Exception as e:
            raise StorageError(f"Failed to write partitioned dataset {full_path}: {e}")

    def exists(self, path: str) -> bool:
        """Check if a file or directory exists."""
        full_path = self._root_dir / path
        return full_path.exists()

    def list_files(self, path: str, pattern: str = "*.parquet") -> list[str]:
        """
        List files matching a pattern in a directory.

        Args:
            path: Relative path from base_path.
            pattern: Glob pattern to match files.

        Returns:
            List of relative paths to matching files.
        """
        full_path = self._root_dir / path
        if not full_path.exists():
            return []

        # Use recursive glob
        glob_pattern = str(full_path / "**" / pattern)
        matches = glob_module.glob(glob_pattern, recursive=True)

        # Convert to relative paths
        return [str(Path(m).relative_to(self._root_dir)) for m in matches]

    def delete(self, path: str) -> bool:
        """
        Delete a file or directory.

        Args:
            path: Relative path from base_path.

        Returns:
            True if deletion was successful, False if path didn't exist.
        """
        full_path = self._root_dir / path

        if not full_path.exists():
            return False

        try:
            if full_path.is_dir():
                shutil.rmtree(full_path)
            else:
                full_path.unlink()
            return True
        except OSError as e:
            raise StorageError(f"Failed to delete {full_path}: {e}")
