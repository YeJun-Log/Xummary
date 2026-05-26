import os
from dotenv import load_dotenv
from google import genai

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SHEET_ID = os.getenv("SHEET_ID")
SUBSCRIBER = os.getenv("SUBSCRIBER")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
APP_PASSWORD = os.getenv("APP_PASSWORD")
BOSS_EMAIL = os.getenv("BOSS_EMAIL")

genai_client = genai.Client(api_key=GEMINI_API_KEY)

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465

NITTER_INSTANCE = "nitter.net"

MODEL_COMMON_REPORT = "gemini-flash-lite-latest"
MODEL_BOSS_REBALANCE = "gemini-3-flash-preview"
