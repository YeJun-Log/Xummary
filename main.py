import logging

from scraper import get_tweets
from summarizer import summarize_text
from mailer import send_email

logger = logging.getLogger(__name__)


def _configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for noisy in ("httpx", "httpcore", "google_genai", "google.auth", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# 메인 함수
if __name__ == "__main__":
    _configure_logging()
    logger.info("Start")
    tweet_data = get_tweets()

    real = 0  #전체 구독자
    test = 3  #테스트용

    if tweet_data:
        summary_result = summarize_text(tweet_data)
        if summary_result:
            send_email(summary_result, test)
        else:
            logger.error("Skipping email send because summarization failed")

    logger.info("End")
