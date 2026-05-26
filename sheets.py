import logging

import pandas as pd

from config import SHEET_ID, SUBSCRIBER

logger = logging.getLogger(__name__)


def _read_sheet(sheet_id, gid=None, label=""):
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    if gid is not None:
        url += f"&gid={gid}"
    try:
        return pd.read_csv(url)
    except Exception as e:
        logger.error("Failed to load sheet (%s): %s", label, e)
        return None


# X 작성자 리스트업
def get_experts_from_sheet():
    df = _read_sheet(SHEET_ID, label="experts")
    if df is None:
        return []
    return df.iloc[:, 0].dropna().map(str).map(lambda x: x.strip()).tolist()


def portfolio():
    df = _read_sheet(SHEET_ID, gid=1238179773, label="portfolio")
    if df is None:
        return "포트폴리오 데이터를 불러올 수 없습니다."

    portfolio_lines = []
    for _, row in df.iterrows():
        # 한 행에서 비어있지 않은 데이터만 뽑아서 리스트로 병합
        row_data = [str(val).strip() for val in row.values if str(val).strip().lower() != 'nan']

        # 유의미한 데이터가 있는 행만 처리
        if len(row_data) > 0:
            portfolio_lines.append(f"- {' | '.join(row_data)}")

    # 전체를 하나의 문자열로 합쳐서 반환
    return "\n".join(portfolio_lines)


# 시트에서 구독자 리스트 뽑기
def get_receivers_from_sheets(who):
    df = _read_sheet(SUBSCRIBER, label="subscribers")
    if df is None:
        return []
    return df.iloc[:, who].dropna().map(str).map(lambda x : x.strip()).tolist()
