import os
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SHEET_ID = os.getenv("SHEET_ID")
SUBSCRIBER = os.getenv("SUBSCRIBER")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
APP_PASSWORD = os.getenv("APP_PASSWORD")
BOSS_EMAIL = os.getenv("BOSS_EMAIL")

_REQUIRED = {
    "GEMINI_API_KEY": GEMINI_API_KEY,
    "SHEET_ID": SHEET_ID,
    "SUBSCRIBER": SUBSCRIBER,
    "SENDER_EMAIL": SENDER_EMAIL,
    "APP_PASSWORD": APP_PASSWORD,
}
_missing = [k for k, v in _REQUIRED.items() if not v]
if _missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(_missing)}")

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465

NITTER_INSTANCE = "nitter.net"

MODEL_COMMON_REPORT = "gemini-flash-lite-latest"
MODEL_BOSS_REBALANCE = "gemini-3-flash-preview"

TWEETS_PER_USER = 5
TPM_COOLDOWN_SEC = 60
IMAGE_DOWNLOAD_TIMEOUT_SEC = 20
PORTFOLIO_GID = 1238179773
SUBSCRIBER_COL_PROD = 0
SUBSCRIBER_COL_TEST = 3
