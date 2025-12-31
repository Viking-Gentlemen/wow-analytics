"""
Configuration loader with TOML support, environment variable overrides, and validation.
"""

from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, field_validator, model_validator, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict, PydanticBaseSettingsSource


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
    """Main application configuration.

    Configuration is loaded in the following priority (highest to lowest):
    1. Environment variables (prefix: WA_, nested delimiter: __)
    2. .env file
    3. config.toml file
    4. Default values
    """

    model_config = SettingsConfigDict(
        env_prefix="WA_",
        env_file=".env",
        env_nested_delimiter="__",
        case_sensitive=False,
        toml_file="config.toml",
    )

    blizzard: BlizzardConfig = Field(default_factory=BlizzardConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Customize settings sources to include TOML file."""
        from pydantic_settings import TomlConfigSettingsSource

        return (
            init_settings,
            env_settings,
            dotenv_settings,
            TomlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


def load_config() -> Config:
    """
    Load configuration from TOML file with environment variable overrides.

    Environment variables follow the pattern:
    - WA_BLIZZARD__API_REGION
    - WA_BLIZZARD__API_CLIENT_ID
    - WA_STORAGE__TYPE
    - WA_STORAGE__LOCAL__ROOT_DIRECTORY

    Returns:
        Validated Config object
    """
    return Config()
