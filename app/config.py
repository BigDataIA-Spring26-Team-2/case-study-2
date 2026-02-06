"""Runtime application configuration."""
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr
from typing import Optional


class SnowflakeConfig(BaseSettings):
    """Snowflake connection for app runtime."""
    
    account: str
    user: str
    password: SecretStr
    warehouse: str
    database: str
    schema: str = Field(default="PUBLIC")
    role: Optional[str] = None
    
    model_config = SettingsConfigDict(
        env_prefix="SNOWFLAKE_",
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )


class RedisConfig(BaseSettings):
    """Redis cache configuration."""
    
    host: str = Field(default="localhost")
    port: int = Field(default=6379)
    db: int = Field(default=0)
    password: Optional[SecretStr] = None
    
    model_config = SettingsConfigDict(
        env_prefix="REDIS_",
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )


class S3Config(BaseSettings):
    """AWS S3 configuration."""
    
    access_key_id: str = Field(default="", alias="AWS_ACCESS_KEY_ID")
    secret_access_key: SecretStr = Field(default="", alias="AWS_SECRET_ACCESS_KEY")
    bucket: str = Field(default="", alias="S3_BUCKET")  # Changed this
    region: str = Field(default="us-east-1", alias="AWS_REGION")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True  
    )


class Settings:
    """Application settings."""
    
    def __init__(self):
        self.snowflake = SnowflakeConfig()
        self.redis = RedisConfig()
        self.s3 = S3Config()


def get_settings() -> Settings:
    """Get application settings."""
    return Settings()