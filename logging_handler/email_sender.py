
# send message if data ingestion fails
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

def send_alert(subject, body):
    # Replace with your email configuration
    sender_email = "your_email@example.com"
    receiver_email = "alert_recipient@example.com"
    password = os.environ.get("EMAIL_PASSWORD")  # Set this as an environment variable

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    message.attach(MIMEText(body, "plain"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())
