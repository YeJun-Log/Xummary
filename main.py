import os
import markdown
import re
from dotenv import load_dotenv
import datetime
import requests
import feedparser
from notion_client import Client as NotionClient
from google import genai
from google.genai import types
from bs4 import BeautifulSoup
import smtplib
from email.mime.text import MIMEText
import pandas as pd

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
SHEET_ID = os.getenv("SHEET_ID")
SUBSCRIBER = os.getenv("SUBSCRIBER")

#Client 설정
genai_client = genai.Client(api_key=GEMINI_API_KEY)
notion = NotionClient(auth=NOTION_TOKEN)

#Email 설정
smtp_server = "smtp.gmail.com"
sender_email = os.getenv("SENDER_EMAIL")
app_password = os.getenv("APP_PASSWORD")

# X 작성자 리스트업
def get_experts_from_sheet():
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

    try:
        df = pd.read_csv(url)
        expert_list = df.iloc[:, 0].dropna().map(str).map(lambda x: x.strip()).tolist()

        return expert_list
    
    except Exception as e:
        print(f"Error in Loading Google Sheets : {e}")
        return []


# 트윗 긁어오기
def get_tweets():
    print("Scrapping Tweets...")
    NITTER_INSTANCE = "nitter.net"
    all_tweet_data = []

    Experts = get_experts_from_sheet()
    for user in Experts:
        rss_url = f"https://{NITTER_INSTANCE}/{user}/rss"
        feed = feedparser.parse(rss_url)
        print(f"🔍 {user} 수집 중... ({len(feed.entries)}개 발견)")
        for entry in feed.entries[:5]: # 최신 순으로 인당 5개 추출해서 요약
            soup = BeautifulSoup(entry.description, "html.parser")
            text_content = soup.get_text().strip()

            img_tag = soup.find('img')
            image_url = None
            if img_tag and img_tag.get('src'):
                image_url = img_tag['src']
                if image_url.startswith('/'):
                    image_url = f"https://{NITTER_INSTANCE}{image_url}"

            all_tweet_data.append({
                "author" : user,
                "text" : text_content,
                "image_url" : image_url
            })

    return all_tweet_data


# 트윗 요약 (Using Gemini)
def summarize_text(tweet_data_list):
    print("Summarizing...")
    instructions = types.Part.from_text(text="""
    # 주간 시장 인사이트 리포트를 작성할거야.
    일주일간 모인 X 게시물을 투자 리서치 노트처럼 재작성해줘.
    데이터의 범위는 지난 7일간의 게시물이야.
    
    [요약 순서]
    [미국 주식 중기] -> [미국 주식 단기] -> [그외 주식] -> [원자재] -> [채권]
    각 순서에 맞는 게시물을 소개해 주고, 각 게시물 분석방법은 아래와 같아.
    
    [분석방법]
    - 원문 내용을 짧게 줄이는 요약은 절대 하지 말 것.
    - 오히려 게시물의 핵심 논리를 더 자세히 풀어 설명할 것, 전문적인 단어는 누구나 이해하기 쉽게 설명해줄 것. 예시를 적극 활용할 것.
    - 참고 그래프를 가져올 수 있으면 제시하고, 없다면 링크를 남길 것.
    - 작성자의 주장에 동의/비동의 여부를 검토하고, 어떤 전제 위에서 성립하는지도 설명할 것.
    - 작성자의 논리적 허점이나 현재의 환경(금리, 물가 등)과의 충돌 지점도 찾아내서 알려줄 것.

    게시물을 모두 요약한 후 지금 시점에서 투자자가 취할 수 있는 행동과
    투자 포트폴리오 비중 확대/축소 아이디어를 제시해줘.
    
    각 분석 끝에 해당 X 게시물의 URL 링크를 반드시 첨부해.
    전문 투자자가 읽는 보고서이므로, 톤앤매너는 진중하게 유지해 줘.
    작성자 및 분석관 이름은 'GnosiCore'로 해줘.
    """)

    contents = [instructions]
    
    for data in tweet_data_list:
        contents.append(types.Part.from_text(text=f"작성자: {data['author']} | 내용: {data['text']}"))
        
        if data['image_url']:
            try:
                img_response = requests.get(data['image_url'], timeout=5)
                if img_response.status_code == 200:
                    contents.append(
                        types.Part.from_bytes(
                            data=img_response.content,
                            mime_type="image/jpeg"
                        )
                    )
            except Exception as e:
                print(f"Error in Downloading Image")

    try:
        response = genai_client.models.generate_content(
            model='gemini-flash-latest', 
            contents=[types.Content(role="user", parts=contents)]
        )
        return response.text
    except Exception as e:
        return f"Error in Summarizing: {e}"

# MarkDown 페이지 변환
def get_rich_text(text):
    parts = []
    tokens = re.split(r'(\*\*.*?\*\*)', text)
    for token in tokens:
        if token.startswith('**') and token.endswith('**'):
            content = token.replace('**', '')
            parts.append({
                "type" : "text",
                "text" : {"content" : content},
                "annotations" : {"bold" : True, "color" : "blue_background"}
            })
        else:
            if token:
                parts.append({"type" : "text", "text" : {"content" : token}})

    return parts

# 노션 페이지 제작
def create_summary_page(content):
    try:
        today = datetime.date.today().strftime("%Y-%m-%d")
        lines = content.split('\n')
        blocks = []

        for line in lines:
            line = line.strip()
            if not line: continue

            if line.startswith('###'):
                blocks.append({
                    "object": "block", "type": "heading_2",
                    "heading_2": {"rich_text": [{"text": {"content": line.replace('###', '').strip()}}]}
                })
                blocks.append({"object": "block", "type": "divider", "divider": {}})

            elif "인사이트" in line:
                blocks.append({
                    "object": "block", "type": "callout",
                    "callout": {
                        "rich_text": get_rich_text(line),
                        "icon": {"emoji": "💡"},
                        "color": "blue_background"
                    }
                })

            elif line.startswith('*') or line.startswith('-'):
                blocks.append({
                    "object": "block", "type": "bulleted_list_item",
                    "bulleted_list_item": {"rich_text": get_rich_text(line.strip('* -'))}
                })

            else:
                blocks.append({
                    "object": "block", "type": "paragraph",
                    "paragraph": {"rich_text": get_rich_text(line)}
                })

        notion.pages.create(
            parent={"database_id": DATABASE_ID},
            properties={
                "이름": { "title": [{"text": {"content": f"{today} X 경제/정치 인텔리전스"}}] }
            },
            children=blocks[:100]
        )
        print("Page Making Complete")
    except Exception as e:
        print(f"Error in Making Page: {e}")

# 시트에서 구독자 리스트 뽑기
def get_receivers_from_sheets():
    url = f"https://docs.google.com/spreadsheets/d/{SUBSCRIBER}/export?format=csv"
    try:
        df = pd.read_csv(url)
        data_list = df.iloc[:, 0].dropna().map(str).map(lambda x : x.strip()).tolist() # 0 : 전체 구독자 / 2 : 테스트 구독자
        return data_list
    
    except Exception as e:
        print(f"Error in Loading Subscriber DB : {e}")
        return []

# 이메일 전송
def send_email(summary_text):

    today = datetime.date.today().strftime("%Y/%m/%d")

    receivers_email = get_receivers_from_sheets()
    if not receivers_email:
        return
    html_content = markdown.markdown(summary_text)
    
    num = 0

    try:
        with smtplib.SMTP_SSL(smtp_server, 465) as server:
            server.login(sender_email, app_password)
            for receiver in receivers_email:
                msg = MIMEText(html_content, 'html')
                msg['Subject'] = f"📊 {today} 국제 정세 트윗 핵심 요약 보고서"
                msg['From'] = sender_email
                msg['To'] = receiver
                server.sendmail(sender_email, receiver, msg.as_string())
                num += 1
        print(f"Complete Sending Mail : {num}")
    except Exception as e:
        print(f"Error in Sending Mail: {e}")


# 메인 함수
if __name__ == "__main__":
    print("Start")
    tweet_data = get_tweets()

    if tweet_data:
       summary_result = summarize_text(tweet_data)
       create_summary_page(summary_result)
       send_email(summary_result)
    print("End")
    