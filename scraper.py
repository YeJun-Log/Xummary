import logging

import feedparser
from bs4 import BeautifulSoup

from config import NITTER_INSTANCE, TWEETS_PER_USER
from sheets import get_experts_from_sheet

logger = logging.getLogger(__name__)


# 트윗 긁어오기
def get_tweets():
    logger.info("Scraping tweets...")
    all_tweet_data = []

    Experts = get_experts_from_sheet()
    if not Experts:
        logger.error("No experts loaded from sheet; aborting tweet collection")
        return []

    global_id = 1

    for user in Experts:
        rss_url = f"https://{NITTER_INSTANCE}/{user}/rss"
        feed = feedparser.parse(rss_url)

        if feed.bozo:
            logger.warning("Nitter parse error for %s: %s", user, feed.bozo_exception)

        logger.info("Collecting %s (%d entries found)", user, len(feed.entries))

        for entry in feed.entries[:TWEETS_PER_USER]: # 최신 순으로 인당 N개 추출해서 요약
            raw_link = entry.link
            x_link = raw_link.replace(NITTER_INSTANCE, "x.com")
            soup = BeautifulSoup(entry.description, "html.parser")
            text_content = soup.get_text().strip()

            img_tag = soup.find('img')
            image_url = None

            if img_tag and img_tag.get('src'):
                image_url = img_tag['src']
                if image_url.startswith('/'):
                    image_url = f"https://{NITTER_INSTANCE}{image_url}"

            all_tweet_data.append({
                "id" : f"{global_id:03d}",
                "author" : user,
                "text" : text_content,
                "image_url" : image_url,
                "link" : x_link
            })
            global_id += 1
    return all_tweet_data
