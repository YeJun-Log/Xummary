import time
import logging

import requests
import google.api_core.exceptions
from google import genai
from google.genai import types, errors
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import (
    GEMINI_API_KEY,
    MODEL_COMMON_REPORT,
    MODEL_BOSS_REBALANCE,
    TPM_COOLDOWN_SEC,
    IMAGE_DOWNLOAD_TIMEOUT_SEC,
)
from sheets import portfolio
from prompts import COMMON_REPORT_INSTRUCTION, BOSS_REBALANCE_PROMPT_TEMPLATE

logger = logging.getLogger(__name__)

genai_client = genai.Client(api_key=GEMINI_API_KEY)

_RETRY_MAX_ATTEMPTS = 10
_RETRY_WAIT_MIN_SEC = 15
_RETRY_WAIT_MAX_SEC = 240
_RETRY_WAIT_MULTIPLIER = 2


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
    stop=stop_after_attempt(_RETRY_MAX_ATTEMPTS),
    wait=wait_exponential(
        multiplier=_RETRY_WAIT_MULTIPLIER,
        min=_RETRY_WAIT_MIN_SEC,
        max=_RETRY_WAIT_MAX_SEC,
    ),
    before_sleep=_log_retry,
)
def safe_generate_content(model_name, contents):
    return genai_client.models.generate_content(model=model_name, contents=contents)


def _build_contents(tweet_data_list):
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
                img_response = requests.get(data['image_url'], timeout=IMAGE_DOWNLOAD_TIMEOUT_SEC)
                if img_response.status_code == 200:
                    contents.append(
                        types.Part.from_bytes(
                            data=img_response.content,
                            mime_type="image/jpeg"
                        )
                    )
            except Exception as e:
                logger.warning("Failed to download image: %s", e)
    return contents


def _summarize_common(tweet_data_list):
    contents = _build_contents(tweet_data_list)
    try:
        response = safe_generate_content(
            MODEL_COMMON_REPORT,
            [types.Content(role="user", parts=contents)]
        )
    except Exception:
        logger.exception("Common summary call failed")
        return None
    text = response.text
    if not text:
        logger.error("Gemini returned empty response for common report")
        return None
    return text


def _summarize_portfolio(common_report, portfolio_text):
    prompt = BOSS_REBALANCE_PROMPT_TEMPLATE.format(
        common_report=common_report,
        portfolio=portfolio_text,
    )
    try:
        response = safe_generate_content(MODEL_BOSS_REBALANCE, prompt)
    except Exception:
        logger.exception("Portfolio call failed")
        return None
    text = response.text
    if not text:
        logger.error("Gemini returned empty response for portfolio")
        return None
    return text


def summarize_text(tweet_data_list):
    logger.info("Summarizing...")

    common_report = _summarize_common(tweet_data_list)
    if common_report is None:
        return None

    logger.info("Completed 1st summary. Waiting %ds before portfolio call", TPM_COOLDOWN_SEC)
    time.sleep(TPM_COOLDOWN_SEC)

    logger.info("Starting portfolio rebalancing call")
    portfolio_text = portfolio()
    boss_analysis = _summarize_portfolio(common_report, portfolio_text)

    if boss_analysis:
        logger.info("Completed portfolio rebalancing")
        boss_content = (
            "#[포트폴리오 전략] \n\n"
            + boss_analysis
            + "\n\n" + "==" * 20 + "\n\n"
            + common_report
        )
    else:
        logger.warning("Portfolio call failed or returned empty; VIPs will receive common report with notice")
        boss_content = (
            "ℹ️ 이번 회차 포트폴리오 리밸런싱 분석 생성에 실패했습니다. 공통 리포트만 전달됩니다.\n\n"
            + "==" * 20 + "\n\n"
            + common_report
        )

    return {"common": common_report, "boss": boss_content}
