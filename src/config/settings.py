from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    # Multi-sport starting seeds
    BASE_URLS: list[str] = [
        # BMBets
        "https://www.bmbets.com/basketball/",
        "https://www.bmbets.com/tennis/",
        "https://www.bmbets.com/volleyball/",
        "https://www.bmbets.com/hockey/",
        "https://www.bmbets.com/american-football/",
        "https://www.bmbets.com/baseball/",
        "https://www.bmbets.com/handball/",
        "https://www.bmbets.com/esports/",
        
        # OddsAgora
        "https://www.oddsagora.com.br/basketball/",
        "https://www.oddsagora.com.br/tennis/",
        "https://www.oddsagora.com.br/volleyball/",
        "https://www.oddsagora.com.br/esports/",
        
        # VitiSport
        "http://www.vitisport.com/index.php?clanek=quicktips&lang=en",
        "http://www.vitisport.com/index.php?clanek=live&lang=en",
        "http://www.vitisport.com/index.php?clanek=basketball&lang=en",
        "http://www.vitisport.com/index.php?clanek=hockey&lang=en",
        "http://www.vitisport.com/index.php?clanek=handball&lang=en",
        
        # SoccerVital (Adding it for completeness, though soccer is de-prioritized)
        "https://www.soccervital.com/"
    ]
    
    # Crawler Constraints
    MAX_PAGES: int = 50000
    CONCURRENCY_LIMIT: int = 100
    REQUEST_DELAY: float = 0.01
    TIMEOUT: float = 5.0
    USER_AGENT: str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

    # Performance & Scaling
    MAX_CONNECTIONS: int = 150
    MAX_KEEPALIVE: int = 150
    BUFFER_SIZE: int = 200
    DNS_CACHE_TTL: int = 3600

    # Storage
    MONGODB_URI: str = "mongodb://localhost:27017"
    DATABASE_NAME: str = "sports_crawler"
    COLLECTION_NAME: str = "odds_data"


settings = Settings()
