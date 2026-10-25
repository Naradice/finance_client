import base64
from datetime import datetime, timedelta, timezone
from email.message import Message
from email.utils import getaddresses
from html.parser import HTMLParser
import logging
import os
from pathlib import Path
import re
import time

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from ..env import load_sbi_env
from .recovery import EmailRecovery, EmailRecoveryError, has_auth_candidate

logger = logging.getLogger(__name__)
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
BASE_PATH = os.path.dirname(__file__)


class GmailConfigurationError(RuntimeError):
    pass


def get_gmail_service():
    load_sbi_env()
    token_path = Path(os.getenv("SBI_GMAIL_TOKEN_FILE", str(Path(BASE_PATH) / "token.json"))).expanduser()
    credentials_path = Path(os.getenv("SBI_GMAIL_CREDENTIALS_FILE", str(Path(BASE_PATH) / "credentials.json"))).expanduser()
    creds = Credentials.from_authorized_user_file(str(token_path), SCOPES) if token_path.is_file() else None
    changed = False
    if creds is not None and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        changed = True
    if creds is None or not creds.valid:
        if not credentials_path.is_file():
            raise GmailConfigurationError("Set SBI_GMAIL_CREDENTIALS_FILE to your Gmail OAuth credentials file")
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
        creds = flow.run_local_server(
            port=0, timeout_seconds=300, authorization_prompt_message="",
            success_message="Gmail authorization completed. You may close this tab.")
        changed = True
    if not creds.has_scopes(SCOPES):
        raise GmailConfigurationError("Gmail read-only permission is required")
    if changed:
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


class _HTMLText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack, self.text = [], []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        hidden = bool(self.stack and self.stack[-1][1]) or tag in ("script", "style", "head", "noscript")
        style = attrs.get("style", "").replace(" ", "").lower()
        hidden = hidden or "hidden" in attrs or attrs.get("aria-hidden") == "true"
        hidden = hidden or "display:none" in style or "visibility:hidden" in style
        if tag in ("br", "hr", "img", "input", "meta", "link", "wbr"):
            if not hidden and tag in ("br", "hr"):
                self.text.append("\n")
            return
        if not hidden and tag in ("p", "div", "tr", "li"):
            self.text.append("\n")
        self.stack.append((tag, hidden, attrs.get("href", "") if tag == "a" else ""))

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i][0] == tag:
                _, hidden, href = self.stack[i]
                del self.stack[i:]
                if not hidden:
                    if href:
                        self.text.append("\n" + href + "\n")
                    elif tag in ("p", "div", "tr", "li"):
                        self.text.append("\n")
                return

    def handle_data(self, data):
        if not self.stack or not self.stack[-1][1]:
            self.text.append(data)


def _header_values(payload, name):
    return [h.get("value", "") for h in payload.get("headers", []) if h.get("name", "").lower() == name.lower()]


def _body(payload, depth=0):
    if depth > 20 or payload.get("filename"):
        return [], []
    if any(value.lower().startswith("attachment") for value in _header_values(payload, "Content-Disposition")):
        return [], []
    plain, rich = [], []
    data = payload.get("body", {}).get("data")
    mime = payload.get("mimeType", "")
    if data and mime in ("text/plain", "text/html"):
        if len(data) > 200000:
            raise EmailRecoveryError("Authentication email part is too large")
        header = Message()
        header["content-type"] = next(iter(_header_values(payload, "Content-Type")), mime)
        charset = header.get_content_charset() or "utf-8"
        try:
            text = base64.urlsafe_b64decode(data + "=" * (-len(data) % 4)).decode(charset)
        except (ValueError, LookupError, UnicodeError):
            raise EmailRecoveryError("Authentication email could not be decoded") from None
        if mime == "text/plain":
            plain.append(text)
        else:
            parser = _HTMLText()
            parser.feed(text)
            rich.append("".join(parser.text))
    for part in payload.get("parts", []):
        child_plain, child_rich = _body(part, depth + 1)
        plain.extend(child_plain)
        rich.extend(child_rich)
    return plain, rich


def message_body(payload):
    plain, rich = _body(payload)
    text = "\n".join(p for p in plain if p.strip()) or "\n".join(rich)
    if len(text) > 100000:
        raise EmailRecoveryError("Authentication email is too large")
    return text


def _sender_matches(payload, expected):
    senders = getaddresses(_header_values(payload, "From"))
    return len(senders) == 1 and senders[0][1].casefold() == expected.casefold()


def _authenticated(payload):
    # Trust the first receiver Authentication-Results from Google's receiving infrastructure.
    results = next((value for value in _header_values(payload, "Authentication-Results")
                    if value.strip().lower().startswith("mx.google.com;")), "")
    for clause in results.lower().split(";"):
        if re.search(r"\bdmarc=pass\b", clause) and re.search(r"\bheader\.from=sbisec\.co\.jp(?=\s|$|\))", clause):
            return True
    return False


def get_latest_email_by_subject(subject_keyword=None, from_email=None, search_from_min_before=1, retry=10,
                                *, not_before=None, require_authenticated=False, body_filter=None):
    service = get_gmail_service()
    lower = not_before or datetime.now(timezone.utc) - timedelta(minutes=search_from_min_before)
    if lower.tzinfo is None:
        raise ValueError("not_before must include a timezone")
    # Actual SBI mail internalDate values are rounded to seconds. Compare at that precision.
    lower = lower.replace(microsecond=0)
    subjects = [subject_keyword] if isinstance(subject_keyword, str) else list(subject_keyword or [])
    quoted = ['subject:"' + item.replace('"', '') + '"' for item in subjects]
    query = (["{" + " ".join(quoted) + "}"] if quoted else [])
    if from_email:
        query.append("from:" + from_email)
    # Gmail's search boundary can exclude a message exactly at this second; check locally below.
    query.append("after:" + str(int(lower.timestamp()) - 1))
    for attempt in range(retry + 1):
        messages = service.users().messages()
        response = messages.list(userId="me", q=" ".join(query), maxResults=20).execute()
        candidates = []
        for item in response.get("messages", []):
            msg = messages.get(userId="me", id=item["id"], format="full").execute()
            date = datetime.fromtimestamp(int(msg.get("internalDate", 0)) / 1000, tz=timezone.utc)
            if not lower <= date <= datetime.now(timezone.utc) + timedelta(seconds=5):
                continue
            payload = msg.get("payload", {})
            if from_email and not _sender_matches(payload, from_email):
                continue
            if require_authenticated and not _authenticated(payload):
                continue
            body = message_body(payload)
            if body_filter is not None and not body_filter(body):
                continue
            candidates.append((date, payload, body))
        if candidates:
            date, payload, body = max(candidates, key=lambda pair: pair[0])
            subject = next(iter(_header_values(payload, "Subject")), "")
            sender = next(iter(_header_values(payload, "From")), "")
            return date, subject, sender, body
        if attempt < retry:
            time.sleep(10)
    return None, None, None, None


def extract_auth_code_from_body(body: str) -> str | None:
    return EmailRecovery.from_env().extract(body, "code")


def _sbi_body(kind, sender="info@sbisec.co.jp", not_before=None, minutes=4):
    if sender.casefold() != "info@sbisec.co.jp":
        raise EmailRecoveryError("SBI authentication requires the configured SBI sender")
    # Search by sender and request time: subject wording is deliberately not fixed.
    _, _, _, body = get_latest_email_by_subject(
        from_email=sender, search_from_min_before=minutes, not_before=not_before, require_authenticated=True,
        body_filter=lambda text: has_auth_candidate(text, kind))
    return body


def retrieve_sbi_device_code(subject="認証コードのお知らせ", sender="info@sbisec.co.jp", *, not_before=None):
    body = _sbi_body("code", sender, not_before=not_before, minutes=1)
    return extract_auth_code_from_body(body) if body else None


def retrieve_code_input_url(*, not_before=None) -> str | None:
    body = _sbi_body("url", not_before=not_before)
    return EmailRecovery.from_env().extract(body, "url") if body else None
