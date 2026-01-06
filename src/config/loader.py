"""
Configuration loader using Pydantic v2 settings.

Supports loading from:
- Environment variables (prefix: WA_)
- .env file
- config.toml file

Example environment variables:
    WA_BLIZZARD__API_REGION=eu
    WA_BLIZZARD__API_CLIENT_ID=your_client_id
    WA_BLIZZARD__API_CLIENT_SECRET=your_secret
    WA_STORAGE__TYPE=local
    WA_STORAGE__LOCAL__ROOT_DIRECTORY=/data/wow-analytics
    WA_STORAGE__S3__BUCKET=my-bucket
    WA_STORAGE__S3__PREFIX=wow-analytics
"""

from enum import Enum
from pathlib import Path
from typing import Any, Optional, Tuple, Type

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)


class BlizzardAPIRegion(str, Enum):
    """Blizzard API regions."""

    EU = "eu"
    US = "us"
    KR = "kr"
    TW = "tw"
    CN = "cn"


class StorageType(str, Enum):
    """Storage backend types."""

    LOCAL = "local"
    S3 = "s3"


class BlizzardConfig(BaseModel):
    """Blizzard API configuration."""

    api_region: BlizzardAPIRegion = BlizzardAPIRegion.EU
    api_base_url: str = "https://eu.api.blizzard.com"
    api_locale: str = "en_GB"
    api_client_id: str = Field(default="", description="Battle.net API client ID")
    api_client_secret: SecretStr = Field(default=SecretStr(""), description="Battle.net API client secret")

    @field_validator("api_client_id")
    @classmethod
    def validate_client_id(cls, v: str) -> str:
        """Validate client ID is not empty when used."""
        return v

    @field_validator("api_base_url")
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        """Validate base URL format."""
        if v and not v.startswith("http"):
            raise ValueError("API base URL must start with http:// or https://")
        return v.rstrip("/")

    @model_validator(mode="after")
    def update_base_url_for_region(self) -> "BlizzardConfig":
        """Update base URL based on region if using default."""
        region_urls = {
            BlizzardAPIRegion.EU: "https://eu.api.blizzard.com",
            BlizzardAPIRegion.US: "https://us.api.blizzard.com",
            BlizzardAPIRegion.KR: "https://kr.api.blizzard.com",
            BlizzardAPIRegion.TW: "https://tw.api.blizzard.com",
            BlizzardAPIRegion.CN: "https://gateway.battlenet.com.cn",
        }
        if self.api_base_url == "https://eu.api.blizzard.com":
            self.api_base_url = region_urls.get(self.api_region, self.api_base_url)
        return self


class LocalStorageConfig(BaseModel):
    """Local filesystem storage configuration."""

    root_directory: Path = Field(
        default=Path("/tmp/wow-analytics"),
        description="Root directory for local Parquet files",
    )
    compression: str = Field(
        default="snappy",
        description="Compression codec (snappy, gzip, zstd, none)",
    )

    @field_validator("root_directory", mode="before")
    @classmethod
    def convert_to_path(cls, v: Any) -> Path:
        """Convert string to Path."""
        if isinstance(v, str):
            return Path(v)
        return v


class S3StorageConfig(BaseModel):
    """S3 storage configuration."""

    bucket: str = Field(default="", description="S3 bucket name")
    prefix: str = Field(default="", description="S3 key prefix (folder)")
    region: str = Field(default="eu-west-1", description="AWS region")
    compression: str = Field(
        default="snappy",
        description="Compression codec (snappy, gzip, zstd, none)",
    )
    endpoint_url: Optional[str] = Field(
        default=None,
        description="Custom S3 endpoint (for MinIO, LocalStack, etc.)",
    )
    aws_access_key_id: Optional[str] = Field(
        default=None,
        description="AWS access key ID (uses env/IAM if not set)",
    )
    aws_secret_access_key: Optional[SecretStr] = Field(
        default=None,
        description="AWS secret access key",
    )


class StorageConfig(BaseModel):
    """Storage configuration - supports local and S3."""

    type: StorageType = Field(
        default=StorageType.LOCAL,
        description="Storage backend type (local or s3)",
    )
    local: LocalStorageConfig = Field(default_factory=LocalStorageConfig)
    s3: S3StorageConfig = Field(default_factory=S3StorageConfig)

    @model_validator(mode="after")
    def validate_storage_config(self) -> "StorageConfig":
        """Validate that the selected storage type has required config."""
        if self.type == StorageType.S3 and not self.s3.bucket:
            raise ValueError("S3 bucket must be specified when using S3 storage")
        return self


class Config(BaseSettings):
    """
    Main application configuration.

    Loads from environment variables (WA_ prefix), .env file, and config.toml.
    """

    model_config = SettingsConfigDict(
        env_prefix="WA_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        toml_file="config.toml",
        extra="ignore",
    )

    blizzard: BlizzardConfig = Field(default_factory=BlizzardConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """Customize settings sources to include TOML."""
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            TomlConfigSettingsSource(settings_cls),
        )

    @classmethod
    def load(cls, toml_path: Optional[str] = None) -> "Config":
        """
        Load configuration from all sources.

        Args:
            toml_path: Optional path to TOML config file (defaults to config.toml).

        Returns:
            Loaded Config instance.
        """
        if toml_path:
            # Override TOML path in model_config
            cls.model_config["toml_file"] = toml_path
        return cls()
