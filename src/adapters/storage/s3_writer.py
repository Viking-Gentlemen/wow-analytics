"""
S3 Parquet Writer - Implementation of ParquetStoragePort for AWS S3.

This adapter writes Parquet files to Amazon S3.
It has NO knowledge of domain-specific data types - it works purely with
DataFrames and PyArrow schemas.

Requires boto3 and s3fs for S3 operations with PyArrow.
"""

from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ports.parquet_storage import ParquetStoragePort, StorageError

try:
    import boto3
    import s3fs

    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False


class S3ParquetWriter(ParquetStoragePort):
    """
    Amazon S3 Parquet storage implementation.

    Writes Parquet files to S3 buckets.
    Supports single file writes and partitioned datasets.

    Requires: pip install boto3 s3fs
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region: str = "eu-west-1",
        default_compression: str = "snappy",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        api_region: Optional[str] = None,
    ):
        """
        Initialize the S3 Parquet writer.

        Args:
            bucket: S3 bucket name.
            prefix: Optional prefix (folder) within the bucket.
            region: AWS region.
            default_compression: Default compression codec.
            aws_access_key_id: Optional AWS access key (uses env/IAM if not provided).
            aws_secret_access_key: Optional AWS secret key.
            endpoint_url: Optional custom endpoint (for S3-compatible storage like MinIO).
            api_region: Optional Blizzard API region to use as root directory prefix.

        Raises:
            StorageError: If S3 dependencies are not installed.
        """
        if not S3_AVAILABLE:
            raise StorageError("S3 support requires boto3 and s3fs. " "Install with: pip install boto3 s3fs")

        self._bucket = bucket
        base_prefix = prefix.strip("/")
        if api_region:
            self._prefix = f"{base_prefix}/{api_region}" if base_prefix else api_region
        else:
            self._prefix = base_prefix
        self._region = region
        self._default_compression = default_compression
        self._endpoint_url = endpoint_url

        # Initialize S3 filesystem
        fs_kwargs = {"anon": False}
        if aws_access_key_id and aws_secret_access_key:
            fs_kwargs["key"] = aws_access_key_id
            fs_kwargs["secret"] = aws_secret_access_key
        if endpoint_url:
            fs_kwargs["client_kwargs"] = {"endpoint_url": endpoint_url}

        self._fs = s3fs.S3FileSystem(**fs_kwargs)

        # Initialize boto3 client for some operations
        client_kwargs = {"region_name": region}
        if aws_access_key_id and aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = aws_access_key_id
            client_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url

        self._s3_client = boto3.client("s3", **client_kwargs)

        # Validate bucket access
        self._validate_bucket()

    def _validate_bucket(self) -> None:
        """Validate that the bucket exists and is accessible."""
        try:
            self._s3_client.head_bucket(Bucket=self._bucket)
        except Exception as e:
            raise StorageError(f"Cannot access S3 bucket {self._bucket}: {e}")

    @property
    def base_path(self) -> str:
        """Return the S3 URI."""
        if self._prefix:
            return f"s3://{self._bucket}/{self._prefix}"
        return f"s3://{self._bucket}"

    def _full_s3_path(self, relative_path: str) -> str:
        """Convert relative path to full S3 path (without s3:// prefix)."""
        parts = [self._bucket]
        if self._prefix:
            parts.append(self._prefix)
        parts.append(relative_path.lstrip("/"))
        return "/".join(parts)

    def _full_s3_uri(self, relative_path: str) -> str:
        """Convert relative path to full S3 URI."""
        return f"s3://{self._full_s3_path(relative_path)}"

    def write(
        self,
        df: pd.DataFrame,
        path: str,
        schema: Optional[pa.Schema] = None,
        partition_cols: Optional[list[str]] = None,
        compression: str = "snappy",
    ) -> str:
        """
        Write a DataFrame to a Parquet file on S3.

        Args:
            df: The DataFrame to write.
            path: Relative path from base_path.
            schema: Optional PyArrow schema.
            partition_cols: Optional list of partition columns.
            compression: Compression codec.

        Returns:
            Full S3 URI where data was written.

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

        s3_uri = self._full_s3_uri(path)
        compression = compression or self._default_compression

        try:
            # Convert DataFrame to PyArrow Table
            if schema:
                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            else:
                table = pa.Table.from_pandas(df, preserve_index=False)

            # Write Parquet file to S3
            with self._fs.open(self._full_s3_path(path), "wb") as f:
                pq.write_table(
                    table,
                    f,
                    compression=compression if compression != "none" else None,
                )

            return s3_uri

        except Exception as e:
            raise StorageError(f"Failed to write Parquet file to S3 {s3_uri}: {e}")

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
        Write a DataFrame as a partitioned Parquet dataset to S3.

        Args:
            df: The DataFrame to write.
            path: Relative path for dataset root.
            partition_cols: Column names to partition by.
            schema: Optional PyArrow schema.
            compression: Compression codec.
            existing_data_behavior: How to handle existing partitions.

        Returns:
            Full S3 URI where dataset was written.

        Raises:
            StorageError: If write fails.
        """
        s3_uri = self._full_s3_uri(path)
        compression = compression or self._default_compression

        try:
            # Convert DataFrame to PyArrow Table
            if schema:
                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            else:
                table = pa.Table.from_pandas(df, preserve_index=False)

            # Write partitioned dataset to S3
            pq.write_to_dataset(
                table,
                root_path=s3_uri,
                partition_cols=partition_cols,
                compression=compression if compression != "none" else None,
                existing_data_behavior=existing_data_behavior,
                filesystem=self._fs,
            )

            return s3_uri

        except Exception as e:
            raise StorageError(f"Failed to write partitioned dataset to S3 {s3_uri}: {e}")

    def exists(self, path: str) -> bool:
        """Check if a file or directory exists on S3."""
        s3_path = self._full_s3_path(path)
        return self._fs.exists(s3_path)

    def list_files(self, path: str, pattern: str = "*.parquet") -> list[str]:
        """
        List files matching a pattern in an S3 path.

        Args:
            path: Relative path from base_path.
            pattern: Glob pattern to match files.

        Returns:
            List of relative paths to matching files.
        """
        s3_path = self._full_s3_path(path)

        if not self._fs.exists(s3_path):
            return []

        try:
            # s3fs glob pattern
            glob_pattern = f"{s3_path}/**/{pattern}"
            matches = self._fs.glob(glob_pattern)

            # Convert to relative paths
            base = self._full_s3_path("")
            return [m.replace(base, "").lstrip("/") for m in matches]

        except Exception:
            return []

    def delete(self, path: str) -> bool:
        """
        Delete a file or directory on S3.

        Args:
            path: Relative path from base_path.

        Returns:
            True if deletion was successful, False if path didn't exist.
        """
        s3_path = self._full_s3_path(path)

        if not self._fs.exists(s3_path):
            return False

        try:
            # s3fs handles both files and directories
            self._fs.rm(s3_path, recursive=True)
            return True
        except Exception as e:
            raise StorageError(f"Failed to delete S3 path {s3_path}: {e}")
