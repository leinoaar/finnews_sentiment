from pydantic_settings import BaseSettings
from pathlib import Path

class Settings(BaseSettings):
    DATABASE_URL: str = "sqlite:///./data/finnews.db"
    TZ: str = "Europe/Rome"
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"

settings = Settings()
DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
