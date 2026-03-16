import pathlib
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    # Server settings
    OUTPUT_DIR: pathlib.Path = pathlib.Path.cwd()
    SOURCE_URL: str = "https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}"
    HOST: str = "0.0.0.0"
    PORT: int = 8100

    # Reverse proxy settings
    REVERSE_PROXY_HOST: Optional[str] = None
    REVERSE_PROXY_PORT: Optional[int] = None

    # Logging
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
