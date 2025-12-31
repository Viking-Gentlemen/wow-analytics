"""
Configuration loader with TOML support, environment variable overrides, and validation.
"""

import os
import tomllib
from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageType(str, Enum):
    """Supported storage types."""

    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"


class BlizzardAPIRegion(str, Enum):
    """Supported Blizzard API regions."""

    US = "us"
    EU = "eu"
    KR = "kr"
    TW = "tw"
    CN = "cn"


class BlizzardConfig(BaseModel):
    """Blizzard API configuration."""

    api_region: BlizzardAPIRegion = Field(default=BlizzardAPIRegion.EU)
    api_base_url: str = Field(default="https://eu.api.blizzard.com")
    api_locale: str = Field(default="en_GB")
    api_client_id: str = Field(default="")
    api_client_secret: SecretStr = Field(default="")

    @field_validator("api_client_id")
    @classmethod
    def validate_client_id(cls, v: str) -> str:
        """Validate that client ID is provided."""
        if not v or v.strip() == "":
            raise ValueError("API client ID must be provided")
        return v

    @field_validator("api_client_secret")
    @classmethod
    def validate_client_secret(cls, v: SecretStr) -> SecretStr:
        """Validate that client secret is provided."""
        secret_value = v.get_secret_value() if isinstance(v, SecretStr) else v
        if not secret_value or secret_value.strip() == "":
            raise ValueError("API client secret must be provided")
        return v

    @field_validator("api_base_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate that URL is properly formatted."""
        if not v.startswith(("http://", "https://")):
            raise ValueError("API base URL must start with http:// or https://")
        return v.rstrip("/")


class LocalStorageConfig(BaseModel):
    """Local storage configuration."""

    root_directory: Path = Field(default=Path("/tmp/wow-analytics-ah"))

    @field_validator("root_directory", mode="before")
    @classmethod
    def validate_directory(cls, v) -> Path:
        """Convert string to Path and validate."""
        path = Path(v) if isinstance(v, str) else v
        return path


class StorageConfig(BaseModel):
    """Storage configuration."""

    type: StorageType = Field(default=StorageType.LOCAL)
    local: LocalStorageConfig = Field(default_factory=LocalStorageConfig)

    @model_validator(mode="after")
    def validate_storage_config(self):
        """Ensure storage-specific config is provided."""
        if self.type == StorageType.LOCAL and not self.local:
            raise ValueError("Local storage configuration is required when type is 'local'")
        return self


class Config(BaseSettings):
    """Main application configuration."""

    model_config = SettingsConfigDict(
        env_prefix="WA_",
        env_file=".env",
        case_sensitive=False,
    )

    blizzard: BlizzardConfig = Field(default_factory=BlizzardConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)


def load_config(config_path: str | Path = "config.toml") -> Config:
    """
    Load configuration from TOML file with environment variable overrides.

    Environment variables follow the pattern:
    - WA_BLIZZARD_API_REGION
    - WA_BLIZZARD_API_CLIENT_ID
    - WA_STORAGE_TYPE
    - WA_STORAGE_LOCAL_ROOT_DIRECTORY

    Args:
        config_path: Path to the TOML configuration file

    Returns:
        Validated Config object

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If configuration is invalid
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    # Load TOML file
    with open(config_path, "rb") as f:
        toml_data = tomllib.load(f)

    # Create config with TOML data as defaults
    # Environment variables will automatically override via pydantic-settings
    config = Config(**toml_data)

    return config
