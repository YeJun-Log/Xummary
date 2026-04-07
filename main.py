import os
import time
import markdown
import datetime
import requests
import smtplib
import feedparser
import pandas as pd
from dotenv import load_dotenv
from google import genai
from google.genai import types, errors
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SHEET_ID = os.getenv("SHEET_ID")
SUBSCRIBER = os.getenv("SUBSCRIBER")

#Client 설정
genai_client = genai.Client(api_key=GEMINI_API_KEY)

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
    global_id = 1

    for user in Experts:
        rss_url = f"https://{NITTER_INSTANCE}/{user}/rss"
        feed = feedparser.parse(rss_url)

        print(f"🔍 {user} 수집 중... ({len(feed.entries)}개 발견)")

        for entry in feed.entries[:5]: # 최신 순으로 인당 5개 추출해서 요약
            raw_link = entry.link
            x_link = raw_link.replace("nitter.net", "x.com")
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


def portfolio():
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=1238179773"
    try:
        df = pd.read_csv(url)
        
        portfolio_lines = []
        for _, row in df.iterrows():
            # 한 행에서 비어있지 않은 데이터만 뽑아서 리스트로 병합
            row_data = [str(val).strip() for val in row.values if str(val).strip().lower() != 'nan']
            
            # 유의미한 데이터가 있는 행만 처리
            if len(row_data) > 0:
                portfolio_lines.append(f"- {' | '.join(row_data)}")
        
        # 전체를 하나의 문자열로 합쳐서 반환
        return "\n".join(portfolio_lines)

    except Exception as e:
        print(f"포트폴리오 로드 중 에러 발생: {e}")
        return "포트폴리오 데이터를 불러올 수 없습니다."


@retry(
    retry=retry_if_exception_type((
        retry_if_exception_type(Exception)
    )),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=10, max=120),
    before_sleep=lambda retry_state: print(f"Error caused by GEMINI : {retry_state.next_action.sleep: .1f}초 후 재시도...({retry_state.attempt_number}회차)")
)
def safe_generate_content(model_name, contents):
    return genai_client.models.generate_content(model=model_name, contents=contents)


# 트윗 요약 (Using Gemini)
def summarize_text(tweet_data_list):
    print("Summarizing...")
    boss_port = portfolio()
    instructions = types.Part.from_text(text="""
    # 주간 시장 인사이트 리포트를 작성할거야.
    일주일간 모인 X 게시물을 투자 리서치 노트처럼 재작성해줘.
    데이터의 범위는 지난 7일간의 게시물이야.
    
    [요약 출력 순서]
    [미국 주식 중기] -> [미국 주식 단기] -> [그외 주식] -> [원자재] -> [채권]
    각 순서에 맞는 게시물을 소개해 주고, 각 게시물 분석방법은 아래와 같아.
    
    [분석방법]
    - 원문 내용을 짧게 줄이는 요약은 절대 하지 말 것.
    - 오히려 게시물의 핵심 논리를 더 자세히 풀어 설명할 것, 전문적인 단어는 누구나 이해하기 쉽게 설명해줄 것. 예시를 적극 활용할 것.
    - 참고 그래프를 가져올 수 있으면 제시하고, 없다면 링크를 남길 것.
    - 작성자의 주장에 동의/비동의 여부를 검토하고, 어떤 전제 위에서 성립하는지도 설명할 것.
    - 작성자의 논리적 허점이나 현재의 환경(금리, 물가 등)과의 충돌 지점도 찾아내서 알려줄 것.
    - 개별 트윗 분석에 그치지 말고, 서로 다른 전문가들 사이에서 공통적으로 언급되는 경제 테마가 있다면 도출해.
    - 전체적인 트윗의 톤앤매너를 분석해서 시장의 심리가 '낙관'인지 '비관'인지, 그 근거는 무엇인지 정리해.
    - 대다수의 의견과 상충하는 독특한 시각이 있다면 무시하지 말고 '특이사항'으로 따로 기록해.
    - 트윗 분석과 더불어 개별 트윗들이 서로 어떻게 영향을 주고받는지 (예 : 채권 금리 상승이 기술주 주가에 미치는 영향 등)를 섹터 간의 상관관계를 중심으로 서술해.        
        
    게시물을 모두 분석한 후 지금 시점에서 투자자가 취하면 좋을 행동도 알려줘.
    
    각 분석 결과의 마지막에는, 해당 분석 대상의 ID와 일치하는 '원본링크'를 수정 없이 토씨 하나 틀리지 않고 그대로 복사하여 첨부해. 
    원본링크가 오류 없이 잘 첨부되었는지 다시 한번 검토 후 답변해.
    절대 스스로 URL을 생성하거나 수정하지 마. 만약 특정 게시물의 링크를 확신할 수 없다면, 링크를 남기지 마.
    전문 투자자가 읽는 보고서이므로, 톤앤매너는 진중하게 유지해 줘.
    답변 전체 어디에도 절대 ID는 적지 마.
    작성자 및 분석관 이름은 'GnosiCore'로 해줘.
    """)
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
                print(f"Error in Downloading Image : {e}")
    try:
        common_response = safe_generate_content(
            'gemini-3-flash-preview', 
            [types.Content(role="user", parts=contents)]
        )
        common_report = common_response.text

        print("Complete 1st summarizing. Wait for 1 minutes")

        time.sleep(60)

        print("Start Making Portfolio")

        pro_prompt = f"""
        너는 개인 투자 수석 분석관이야. 
        아래의 [주간 시장 인사이트 리포트]를 읽고, [현재 포트폴리오]에 맞춘 
        구체적인 리밸런싱 아이디어를 제안해 줘.
        구체적 수치는 반드시 마크다운 표 형식을 지켜서 작성한 후 제시해줘.
        표의 열은 [자산명, 현재 비중, 목표 비중, 변동, 핵심 행동]으로 제한해.
        **[주의]** 표의 각 셀(Cell)에는 문장이 아닌 '단어'나 '짧은 구' 위주로 작성하여 표가 옆으로 깨지지 않게 할 것.
        상세한 이유나 설명은 표 아래에 불렛포인트로 따로 뺄 것.
        표의 구분선을 반드시 포함하여 마크다운 규격을 준수할 것.

        [주간 시장 인사이트 리포트]
        {common_report}
        
        [현재 포트폴리오]
        {boss_port}
        
        지침:
        - 시장 리포트의 핵심 내용이 현재 보유 자산(금, 원유, 주식 등)에 미칠 영향을 분석할 것.
        - 현재 비중에서 무엇을 늘리고 무엇을 줄여야 할지 '행동 지침'을 명확히 할 것.
        - 진중하고 전문적인 톤으로 작성할 것.
        - 쓸데없는 사족을 달지 않고, 최대한 간결하며 핵심을 잘 짚을 것. 
        - 해당 내용이 정확한지 답변 생성 전 다시 검토할 것.
        - 절대 거짓말을 하지 않을 것.
        """
        boss_reponse = safe_generate_content(
            'gemini-3-flash-preview',
            pro_prompt
        )
        boss_analysis = boss_reponse.text
        print("Complete Portfolio Making")
        return {
            "common": common_report,
            "boss": "#[포트폴리오 전략] \n\n" + boss_analysis + "\n\n" + "==" * 20 + "\n\n" + common_report
        }
    except Exception as e:
        print (f"Error in Summarizing: {e}")
        return None




# 시트에서 구독자 리스트 뽑기
def get_receivers_from_sheets(who):
    url = f"https://docs.google.com/spreadsheets/d/{SUBSCRIBER}/export?format=csv"
    try:
        df = pd.read_csv(url)
        data_list = df.iloc[:, who].dropna().map(str).map(lambda x : x.strip()).tolist() 
        return data_list
    except Exception as e:
        print(f"Error in Loading Subscriber DB : {e}")
        return []




# 이메일 전송
def send_email(summary_dict, who):
    today = datetime.date.today().strftime("%Y/%m/%d")
    receivers_email = get_receivers_from_sheets(who)

    BOSS_EMAIL = os.getenv("SENDER_EMAIL")

    if not receivers_email:
        return
    
    num = 0
    try:
        with smtplib.SMTP_SSL(smtp_server, 465) as server:
            server.login(sender_email, app_password)
            for receiver in receivers_email:
                if receiver == BOSS_EMAIL:
                    content = summary_dict["boss"]
                    subject = f"{today} [VIP 전용] 맞춤형 투자 리포트"
                else:
                    content = summary_dict["common"]
                    subject = f"📊 {today} 경제 추세 핵심 보고서"

                html_content = markdown.markdown(content, extensions=['tables'])
                msg = MIMEText(html_content, 'html')
                msg['Subject'] = subject
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

    real = 0  #전체 구독자
    test = 3  #테스트용

    if tweet_data:
        summary_result = summarize_text(tweet_data)
        if summary_result:
            send_email(summary_result, test)
        else:
            print("Error, Don't send email...")           

    print("End")
    