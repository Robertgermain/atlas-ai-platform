"""Application configuration loaded from the environment."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings for Atlas infrastructure."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="ATLAS_",
        extra="ignore",
    )

    database_url: str = Field(
        default="postgresql+psycopg://atlas:atlas@127.0.0.1:5433/atlas",
    )


def get_settings() -> Settings:
    """Return settings from the current environment."""
    return Settings()
