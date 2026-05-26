import smtplib
import datetime
import logging

import markdown
from email.mime.text import MIMEText

from config import SMTP_SERVER, SMTP_PORT, SENDER_EMAIL, APP_PASSWORD, BOSS_EMAIL
from sheets import get_receivers_from_sheets

logger = logging.getLogger(__name__)


# 이메일 전송
def send_email(summary_dict, who):
    today = datetime.date.today().strftime("%Y/%m/%d")
    receivers_email = get_receivers_from_sheets(who)

    if not receivers_email:
        return

    num = 0
    vip = 0
    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(SENDER_EMAIL, APP_PASSWORD)
            for receiver in receivers_email:
                if (receiver == BOSS_EMAIL) or (receiver == SENDER_EMAIL):
                    content = summary_dict["boss"]
                    subject = f"{today} [VIP 전용] 맞춤형 투자 리포트"
                    vip += 1
                else:
                    content = summary_dict["common"]
                    subject = f"📊 {today} 경제 추세 핵심 보고서"
                    num += 1

                html_content = markdown.markdown(content, extensions=['tables'])
                msg = MIMEText(html_content, 'html')
                msg['Subject'] = subject
                msg['From'] = SENDER_EMAIL
                msg['To'] = receiver
                server.sendmail(SENDER_EMAIL, receiver, msg.as_string())

        logger.info("Sent mail: common=%d, vip=%d", num, vip)
    except Exception as e:
        logger.exception("Error sending mail: %s", e)
