import time
import logging

import requests
import google.api_core.exceptions
from google.genai import types, errors
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import genai_client, MODEL_COMMON_REPORT, MODEL_BOSS_REBALANCE
from sheets import portfolio
from prompts import COMMON_REPORT_INSTRUCTION, BOSS_REBALANCE_PROMPT_TEMPLATE

logger = logging.getLogger(__name__)


def _log_retry(retry_state):
    logger.warning(
        "Gemini retry in %.1fs (attempt %d)",
        retry_state.next_action.sleep,
        retry_state.attempt_number,
    )


@retry(
    retry=retry_if_exception_type((
        errors.APIError,
        google.api_core.exceptions.InternalServerError,
        google.api_core.exceptions.ServiceUnavailable,
        google.api_core.exceptions.ResourceExhausted,
        google.api_core.exceptions.DeadlineExceeded
    )),
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=15, max=240), # 15초부터 2배씩 늘려가며 대기
    before_sleep=_log_retry,
)
def safe_generate_content(model_name, contents):
    return genai_client.models.generate_content(model=model_name, contents=contents)


# 트윗 요약 (Using Gemini)
def summarize_text(tweet_data_list):
    logger.info("Summarizing...")
    boss_port = portfolio()
    instructions = types.Part.from_text(text=COMMON_REPORT_INSTRUCTION)
    contents = [instructions]
    for data in tweet_data_list:
        contents.append(
            types.Part.from_text(
                text=f"[ID: {data['id']}] | 작성자: {data['author']} | 내용: {data['text']} | 원본링크: {data['link']}"
            )
        )
        if data['image_url']:
            try:
                img_response = requests.get(data['image_url'], timeout=20)
                if img_response.status_code == 200:
                    contents.append(
                        types.Part.from_bytes(
                            data=img_response.content,
                            mime_type="image/jpeg"
                        )
                    )
            except Exception as e:
                logger.warning("Failed to download image: %s", e)
    try:
        common_response = safe_generate_content(
            MODEL_COMMON_REPORT,
            [types.Content(role="user", parts=contents)]
        )
        common_report = common_response.text
        if not common_report:
            logger.error("Gemini returned empty response for common report")
            return None

        logger.info("Completed 1st summary. Waiting 60s before portfolio call")

        time.sleep(60)

        logger.info("Starting portfolio rebalancing call")

        pro_prompt = BOSS_REBALANCE_PROMPT_TEMPLATE.format(
            common_report=common_report,
            portfolio=boss_port,
        )
        boss_response = safe_generate_content(
            MODEL_BOSS_REBALANCE,
            pro_prompt
        )
        boss_analysis = boss_response.text
        logger.info("Completed portfolio rebalancing")
        return {
            "common": common_report,
            "boss": "#[포트폴리오 전략] \n\n" + boss_analysis + "\n\n" + "==" * 20 + "\n\n" + common_report
        }
    except Exception as e:
        logger.exception("Error in summarizing: %s", e)
        return None
