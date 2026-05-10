"""Config via pydantic-settings. Three env vars total."""

from functools import cached_property

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(
        ..., description="Superuser connection to a postgres server."
    )
    redis_url: str = Field(..., description="Admin connection to a redis server.")
    http_basic_auth: str = Field(..., description="Format: 'username:password'.")

    @field_validator("http_basic_auth")
    @classmethod
    def _validate_auth(cls, v: str) -> str:
        if ":" not in v:
            raise ValueError(
                "HTTP_BASIC_AUTH must contain a colon separating username and password"
            )
        username, _, password = v.partition(":")
        if not username or not password:
            raise ValueError(
                "HTTP_BASIC_AUTH username and password must both be non-empty"
            )
        return v

    @cached_property
    def auth_username(self) -> str:
        return self.http_basic_auth.split(":", 1)[0]

    @cached_property
    def auth_password(self) -> str:
        # split with maxsplit=1 so colons in the password survive
        return self.http_basic_auth.split(":", 1)[1]


def load_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
