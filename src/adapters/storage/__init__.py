"""Storage adapters - Implementations of ParquetStoragePort."""

from adapters.storage.local_writer import LocalParquetWriter
from adapters.storage.s3_writer import S3ParquetWriter

__all__ = ["LocalParquetWriter", "S3ParquetWriter"]
