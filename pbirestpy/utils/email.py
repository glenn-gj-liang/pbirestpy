import smtplib
from email.message import EmailMessage


def send_email(
    receiver: str, sender: str, subject: str, body: str, smtp_server: str
):
    msg = EmailMessage()
    msg["subject"] = subject
    msg["from"] = sender
    msg["to"] = receiver
    msg.set_content(body)

    smtp = smtplib.SMTP(smtp_server)
    try:
        smtp.send_message(msg)
    except Exception as e:
        print(f"Failed to send email: {e}")
    finally:
        smtp.quit()
