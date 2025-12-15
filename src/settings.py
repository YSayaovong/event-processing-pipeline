import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    DATABASE_URL: str = os.environ["DATABASE_URL"]

    PIPELINE_NAME: str = os.environ["PIPELINE_NAME"]
    SOURCE_NAME: str = os.environ["SOURCE_NAME"]

    RAW_DATA_PATH: str = os.environ["RAW_DATA_PATH"]

    LATE_ARRIVAL_GRACE_MINUTES: int = int(
        os.environ.get("LATE_ARRIVAL_GRACE_MINUTES", "60")
    )

    # ---- Phase 2: Resilience controls ----
    FAIL_MODE: str = os.environ.get("FAIL_MODE", "none").lower()
    MAX_RETRIES: int = int(os.environ.get("MAX_RETRIES", "4"))


settings = Settings()
