import base64
import os
import re
from datetime import datetime, timezone

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
BASE_PATH = os.path.dirname(__file__)


def get_gmail_service():
    creds = None
    token_file_path = os.path.join(BASE_PATH, "token.json")
    if os.path.exists(token_file_path):
        creds = Credentials.from_authorized_user_file(token_file_path, SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            os.path.join(BASE_PATH, "credentials.json"), SCOPES
        )
        creds = flow.run_local_server(port=0)
        with open(token_file_path, "w") as token:
            token.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def get_latest_email_by_subject(subject_keyword, from_email=None):
    service = get_gmail_service()

    date = None
    subject = None
    sender = None
    body = None

    query_parts = [f'subject:"{subject_keyword}"']
    if from_email:
        query_parts.append(f"from:{from_email}")
    query = " ".join(query_parts)
    results = (
        service.users()
        .messages()
        .list(userId="me", q=query, labelIds=["INBOX"])
        .execute()
    )
    messages = results.get("messages", [])

    if not messages:
        print("該当するメールは見つかりませんでした。")
        return date, subject, sender, body

    latest_message = None
    latest_internal_date = 0

    for msg_data in messages:
        msg = (
            service.users()
            .messages()
            .get(userId="me", id=msg_data["id"], format="full")
            .execute()
        )
        internal_date = int(msg.get("internalDate", 0))  # Unix timestamp in ms

        if internal_date > latest_internal_date:
            latest_internal_date = internal_date
            latest_message = msg

    if latest_message is None:
        print("該当するメールは見つかりませんでした。")
        return date, subject, sender, body

    payload = latest_message["payload"]
    headers = payload["headers"]

    subject = next(
        (h["value"] for h in headers if h["name"] == "Subject"), "No Subject"
    )
    sender = next(
        (h["value"] for h in headers if h["name"] == "From"), "Unknown Sender"
    )
    date = datetime.fromtimestamp(latest_internal_date / 1000, tz=timezone.utc)

    # 本文の取得（プレーンテキスト優先）
    body = ""
    if "data" in payload["body"]:
        body = base64.urlsafe_b64decode(payload["body"]["data"]).decode(
            "utf-8", errors="ignore"
        )
    else:
        for part in payload.get("parts", []):
            if part["mimeType"] == "text/plain":
                body = base64.urlsafe_b64decode(part["body"]["data"]).decode(
                    "utf-8", errors="ignore"
                )
                break

    return date, subject, sender, body


def extract_auth_code_from_body(body: str) -> str | None:
    # 改行や空白を挟んだ「認証コード」の後の英数字（例: A0123）を抽出
    match = re.search(r"認証コード\s*[\r\n　]+([A-Z0-9]{4,})", body)
    if match:
        return match.group(1)
    print(f"no match in {body}")
    return None


def retrieve_sbi_device_code(
    subject="認証コードのお知らせ", sender="info@sbisec.co.jp"
):
    date, subject, sender, body = get_latest_email_by_subject(subject, sender)
    if body:
        return extract_auth_code_from_body(body)
    print("body is missing")
    return None


if __name__ == "__main__":
    print(retrieve_sbi_device_code())
