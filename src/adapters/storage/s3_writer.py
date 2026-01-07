"""
S3 Parquet Writer - Implementation of ParquetStoragePort for AWS S3.

This adapter writes Parquet files to Amazon S3 or S3-compatible storage
(like Cloudflare R2, MinIO, etc.).
It has NO knowledge of domain-specific data types - it works purely with
DataFrames and PyArrow schemas.

Uses boto3 and PyArrow's native S3 filesystem for S3 operations.
"""

import fnmatch
import io
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from ports.parquet_storage import ParquetStoragePort, StorageError

try:
    import boto3
    from botocore.exceptions import ClientError

    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False


class S3ParquetWriter(ParquetStoragePort):
    """
    Amazon S3 Parquet storage implementation.

    Writes Parquet files to S3 buckets or S3-compatible storage.
    Supports single file writes and partitioned datasets.

    Uses boto3 for basic operations and PyArrow's native S3FileSystem
    for partitioned dataset writes, which provides better compatibility
    with S3-compatible storage like Cloudflare R2.

    Requires: pip install boto3 pyarrow
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
            endpoint_url: Optional custom endpoint (for S3-compatible storage like R2, MinIO).
            api_region: Optional Blizzard API region to use as root directory prefix.

        Raises:
            StorageError: If S3 dependencies are not installed.
        """
        if not S3_AVAILABLE:
            raise StorageError("S3 support requires boto3. Install with: pip install boto3")

        self._bucket = bucket
        base_prefix = prefix.strip("/")
        if api_region:
            self._prefix = f"{base_prefix}/{api_region}" if base_prefix else api_region
        else:
            self._prefix = base_prefix
        self._region = region
        self._default_compression = default_compression
        self._endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key

        # Initialize boto3 client
        client_kwargs = {"region_name": region}
        if aws_access_key_id and aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = aws_access_key_id
            client_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url

        self._s3_client = boto3.client("s3", **client_kwargs)

        # Initialize PyArrow S3FileSystem for partitioned writes
        self._pa_fs = self._create_pyarrow_filesystem()

        # Validate bucket access
        self._validate_bucket()

    def _create_pyarrow_filesystem(self) -> pafs.S3FileSystem:
        """Create a PyArrow S3FileSystem for partitioned dataset operations."""
        fs_kwargs = {"region": self._region}

        if self._aws_access_key_id and self._aws_secret_access_key:
            fs_kwargs["access_key"] = self._aws_access_key_id
            fs_kwargs["secret_key"] = self._aws_secret_access_key

        if self._endpoint_url:
            # Parse endpoint URL to extract host and scheme
            from urllib.parse import urlparse

            parsed = urlparse(self._endpoint_url)
            fs_kwargs["endpoint_override"] = parsed.netloc
            if parsed.scheme == "http":
                fs_kwargs["scheme"] = "http"

        return pafs.S3FileSystem(**fs_kwargs)

    def _validate_bucket(self) -> None:
        """Validate that the bucket exists and is accessible."""
        try:
            self._s3_client.head_bucket(Bucket=self._bucket)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                raise StorageError(f"S3 bucket {self._bucket} does not exist")
            elif error_code == "403":
                raise StorageError(f"Access denied to S3 bucket {self._bucket}")
            else:
                raise StorageError(f"Cannot access S3 bucket {self._bucket}: {e}")
        except Exception as e:
            raise StorageError(f"Cannot access S3 bucket {self._bucket}: {e}")

    @property
    def base_path(self) -> str:
        """Return the S3 URI."""
        if self._prefix:
            return f"s3://{self._bucket}/{self._prefix}"
        return f"s3://{self._bucket}"

    def _full_s3_key(self, relative_path: str) -> str:
        """Convert relative path to full S3 key (without bucket)."""
        parts = []
        if self._prefix:
            parts.append(self._prefix)
        parts.append(relative_path.lstrip("/"))
        return "/".join(parts)

    def _full_s3_uri(self, relative_path: str) -> str:
        """Convert relative path to full S3 URI."""
        key = self._full_s3_key(relative_path)
        return f"s3://{self._bucket}/{key}"

    def _pyarrow_path(self, relative_path: str) -> str:
        """Convert relative path to PyArrow filesystem path (bucket/key format)."""
        key = self._full_s3_key(relative_path)
        return f"{self._bucket}/{key}"

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
        s3_key = self._full_s3_key(path)
        compression = compression or self._default_compression

        try:
            # Convert DataFrame to PyArrow Table
            if schema:
                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            else:
                table = pa.Table.from_pandas(df, preserve_index=False)

            # Write Parquet to in-memory buffer
            buffer = io.BytesIO()
            pq.write_table(
                table,
                buffer,
                compression=compression if compression != "none" else None,
            )
            buffer.seek(0)

            # Upload to S3
            self._s3_client.put_object(
                Bucket=self._bucket,
                Key=s3_key,
                Body=buffer.getvalue(),
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
        pa_path = self._pyarrow_path(path)
        compression = compression or self._default_compression

        try:
            # Convert DataFrame to PyArrow Table
            if schema:
                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            else:
                table = pa.Table.from_pandas(df, preserve_index=False)

            # Write partitioned dataset using PyArrow's native S3 filesystem
            pq.write_to_dataset(
                table,
                root_path=pa_path,
                partition_cols=partition_cols,
                compression=compression if compression != "none" else None,
                existing_data_behavior=existing_data_behavior,
                filesystem=self._pa_fs,
            )

            return s3_uri

        except Exception as e:
            raise StorageError(f"Failed to write partitioned dataset to S3 {s3_uri}: {e}")

    def exists(self, path: str) -> bool:
        """Check if a file or directory exists on S3."""
        s3_key = self._full_s3_key(path)

        # Check if it's a file
        try:
            self._s3_client.head_object(Bucket=self._bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "404":
                raise StorageError(f"Error checking existence of {s3_key}: {e}")

        # Check if it's a directory (prefix with objects)
        try:
            response = self._s3_client.list_objects_v2(
                Bucket=self._bucket,
                Prefix=s3_key.rstrip("/") + "/",
                MaxKeys=1,
            )
            return response.get("KeyCount", 0) > 0
        except Exception as e:
            raise StorageError(f"Error checking existence of {s3_key}: {e}")

    def list_files(self, path: str, pattern: str = "*.parquet") -> list[str]:
        """
        List files matching a pattern in an S3 path.

        Args:
            path: Relative path from base_path.
            pattern: Glob pattern to match files.

        Returns:
            List of relative paths to matching files.
        """
        s3_prefix = self._full_s3_key(path)
        if s3_prefix and not s3_prefix.endswith("/"):
            s3_prefix += "/"

        try:
            matching_files = []
            paginator = self._s3_client.get_paginator("list_objects_v2")

            for page in paginator.paginate(Bucket=self._bucket, Prefix=s3_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    filename = key.split("/")[-1]

                    # Match against pattern
                    if fnmatch.fnmatch(filename, pattern):
                        # Convert to relative path
                        base_prefix = self._full_s3_key("")
                        if base_prefix:
                            relative = key[len(base_prefix) :].lstrip("/")
                        else:
                            relative = key
                        matching_files.append(relative)

            return matching_files

        except Exception as e:
            raise StorageError(f"Error listing files in {s3_prefix}: {e}")

    def delete(self, path: str) -> bool:
        """
        Delete a file or directory on S3.

        Args:
            path: Relative path from base_path.

        Returns:
            True if deletion was successful, False if path didn't exist.
        """
        s3_key = self._full_s3_key(path)

        try:
            # Try to delete as a single object first
            try:
                self._s3_client.head_object(Bucket=self._bucket, Key=s3_key)
                self._s3_client.delete_object(Bucket=self._bucket, Key=s3_key)
                return True
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") != "404":
                    raise

            # Try to delete as a prefix (directory)
            prefix = s3_key.rstrip("/") + "/"
            paginator = self._s3_client.get_paginator("list_objects_v2")
            objects_to_delete = []

            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    objects_to_delete.append({"Key": obj["Key"]})

            if not objects_to_delete:
                return False

            # Delete in batches of 1000 (S3 limit)
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i : i + 1000]
                self._s3_client.delete_objects(
                    Bucket=self._bucket,
                    Delete={"Objects": batch},
                )

            return True

        except Exception as e:
            raise StorageError(f"Failed to delete S3 path {s3_key}: {e}")
