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
from email.mime.multipart import MIMEMultipart
import pandas as pd

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
SHEET_ID = os.getenv("SHEET_ID")

#Client 설정
genai_client = genai.Client(api_key=GEMINI_API_KEY)
notion = NotionClient(auth=NOTION_TOKEN)

#Email 설정
smtp_server = "smtp.gmail.com"
sender_email = os.getenv("SENDER_EMAIL")
app_password = os.getenv("APP_PASSWORD")
receivers_email = os.getenv("RECEIVERS_EMAIL")

#ListUp
def get_experts_from_sheet():
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

    try:
        df = pd.read_csv(url)
        expert_list = df.iloc[:, 0].dropna().map(str).map(lambda x: x.strip()).tolist()

        return expert_list
    
    except Exception as e:
        print(f"Error in Loading Google Sheets : {e}")
        return []


#Get Tweets From X
def get_tweets():
    print("Scrapping Tweets...")
    NITTER_INSTANCE = "nitter.net"
    all_tweet_data = []

    Experts = get_experts_from_sheet()
    for user in Experts:
        rss_url = f"https://{NITTER_INSTANCE}/{user}/rss"
        feed = feedparser.parse(rss_url)
        print(f"🔍 {user} 수집 중... ({len(feed.entries)}개 발견)")
        for entry in feed.entries[:4]:
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


#Summarize tweets using GEMINI
def summarize_text(tweet_data_list):
    print("Summarizing...")
    instructions = types.Part.from_text(text="""
    너는 경제 및 정치 분야의 전문 정보 분석관이야. 
    제공된 트윗 텍스트와 첨부된 이미지들을 분석해서 전문가 수준의 '인텔리전스 보고서'를 작성해줘.

    [분석 지침]
    1. 주요 이슈(Headline)를 선정해서 알려줘.
    2. 시각적 분석(Visual Insights): 첨부된 이미지가 '차트'라면 수치를 해석하고, '현장 사진'이라면 상황을 텍스트와 연결해 분석해줘. (이미지가 없다면 이 항목은 제외)
    3. 대립 의견(Dissenting Voices): 전문가들 사이에서 의견이 갈리는 맥락이 있다면, 반드시 포착해줘.
    4. 향후 전망: 이 정보들이 향후 시장이나 정책에 줄 영향과 주의할 리스크를 정리해줘.

    [출력 형식]
    - 한국어로 작성, 중요한 단어는 [](대괄호) 사용
    - 마지막에 '오늘의 한 줄 인사이트' 남기기.
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

#MarkDown 페이지 변환
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

#Making Notion Page
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

#Sending Email to Receivers
def send_email(summary_text):

    if not receivers_email:
        return
    receiver_email = [r.strip() for r in receivers_email.split(",") if r.strip()]
    html_content = markdown.markdown(summary_text)
    
    try:
        with smtplib.SMTP_SSL(smtp_server, 465) as server:
            server.login(sender_email, app_password)
            for receiver in receiver_email:
                msg = MIMEText(html_content, 'html')
                msg['Subject'] = "📊 금주 핵심 요약 보고서"
                msg['From'] = sender_email
                msg['To'] = receiver
                server.sendmail(sender_email, receiver, msg.as_string())
        print("Complete Sending Mail")
    except Exception as e:
        print(f"Error in Sending Mail: {e}")

if __name__ == "__main__":
    print("Start")
    tweet_data = get_tweets()
    if tweet_data:
        summary_result = summarize_text(tweet_data)
        create_summary_page(summary_result)
        send_email(summary_result)
    print("End")