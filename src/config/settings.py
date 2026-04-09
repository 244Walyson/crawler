from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    # Crawler Seeds derived from RESULT.md research
    BASE_URLS: list[str] = [
        "https://www.bmbets.com/",
        "https://www.oddsagora.com.br/",
        "http://www.vitisport.com/index.php?clanek=quicktips&lang=en",
        "https://www.soccervital.com/",
        "https://www.xscores.com/soccer"
    ]

    # Crawler Constraints
    MAX_PAGES: int = 50000
    CONCURRENCY_LIMIT: int = 50
    REQUEST_DELAY: float = 0.5
    TIMEOUT: float = 5.0
    USER_AGENT: str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

    # Performance & Scaling
    MAX_CONNECTIONS: int = 100
    MAX_KEEPALIVE: int = 50
    BUFFER_SIZE: int = 200
    DNS_CACHE_TTL: int = 3600

    # Storage
    MONGODB_URI: str = "mongodb://localhost:27017"
    DATABASE_NAME: str = "sports_crawler"
    COLLECTION_NAME: str = "odds_data"

    # Distributed
    REDIS_URL: str = "redis://localhost:6379"
    HEADLESS: bool = False


settings = Settings()
