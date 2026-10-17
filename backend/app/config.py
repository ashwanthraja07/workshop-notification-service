"""
Application configuration using Pydantic v2 BaseSettings.

Design decisions:
  • All config comes from environment variables so the app is 12-factor
    compliant — no secrets in source.
  • We group dispatcher tuning params here so they can be overridden per
    environment without code changes (e.g. faster polling in tests).
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # -------------------------------------------------------------------------
    # Database
    # -------------------------------------------------------------------------
    database_url: str = "postgresql://workshop:workshop@localhost:5432/workshop_db"
    # asyncpg pool sizing — min keeps connections warm, max caps resource use
    db_pool_min: int = 2
    db_pool_max: int = 10

    # -------------------------------------------------------------------------
    # Dispatcher (outbox worker)
    # -------------------------------------------------------------------------
    # How often the dispatcher polls for due rows (seconds)
    dispatcher_poll_interval: float = 2.0
    # How long a lease lasts before a crashed worker's row is reclaimed (seconds)
    dispatcher_lease_seconds: int = 30
    # Maximum concurrent notification sends per poll cycle
    dispatcher_concurrency: int = 10
    # Base delay for exponential backoff (seconds)
    dispatcher_backoff_base: float = 2.0
    # Cap on backoff delay (seconds)
    dispatcher_backoff_max: float = 300.0
    # Jitter range as a fraction of the computed delay (0.25 = ±25%)
    dispatcher_jitter_factor: float = 0.25

    # -------------------------------------------------------------------------
    # Notification service
    # -------------------------------------------------------------------------
    notifier_url: str = "http://mock_notifier:8001"
    # Short timeout so slow responses are treated as errors quickly
    notifier_timeout: float = 5.0


settings = Settings()
