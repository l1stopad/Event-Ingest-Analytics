from pydantic import BaseModel, Field
import os

class Settings(BaseModel):
    app_name: str = "events-analytics"
    env: str = Field(default=os.getenv("ENV", "dev"))
    log_level: str = Field(default=os.getenv("LOG_LEVEL", "INFO"))
    # DB
    db_host: str = Field(default=os.getenv("POSTGRES_HOST", "postgres"))
    db_port: int = Field(default=int(os.getenv("POSTGRES_PORT", "5432")))
    db_user: str = Field(default=os.getenv("POSTGRES_USER", "app"))
    db_password: str = Field(default=os.getenv("POSTGRES_PASSWORD", "app"))
    db_name: str = Field(default=os.getenv("POSTGRES_DB", "events"))
    db_pool_min: int = Field(default=int(os.getenv("DB_POOL_MIN", "1")))
    db_pool_max: int = Field(default=int(os.getenv("DB_POOL_MAX", "10")))

    # Metrics
    enable_metrics: bool = Field(default=os.getenv("ENABLE_METRICS", "1") == "1")

    # Rate limit (буде використано на етапі 4)
    rate_limit_rps: int = Field(default=int(os.getenv("RATE_LIMIT_RPS", "20")))
    rate_limit_burst: int = Field(default=int(os.getenv("RATE_LIMIT_BURST", "40")))

settings = Settings()
