import json
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _strip_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        return s[1:-1]
    return s


@dataclass(frozen=True)
class Settings:
    blog_base_url: str
    sanity_project_id: str
    sanity_dataset: str
    google_service_account_key: dict
    spreadsheet_id: str
    contact_email: str
    google_sheets_scopes: tuple[str, ...]


def load_settings() -> Settings:
    base = os.environ.get("BLOG_BASE_URL", "https://xepelin.com/blog").rstrip("/")
    raw_key = os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY")
    if not raw_key:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY is required")
    key_str = _strip_quotes(raw_key)
    try:
        sa = json.loads(key_str)
    except json.JSONDecodeError as e:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY must be valid JSON") from e

    sid = os.environ.get("SPREADSHEET_ID")
    if not sid:
        raise ValueError("SPREADSHEET_ID is required")

    email = os.environ.get("CONTACT_EMAIL")
    if not email:
        raise ValueError("CONTACT_EMAIL is required")

    scope = os.environ.get(
        "GOOGLE_SHEETS_SCOPES", "https://www.googleapis.com/auth/spreadsheets"
    )
    scopes = tuple(s.strip() for s in scope.split(",") if s.strip())

    return Settings(
        blog_base_url=base,
        sanity_project_id=os.environ.get("SANITY_PROJECT_ID", "4n68r2aa"),
        sanity_dataset=os.environ.get("SANITY_DATASET", "production"),
        google_service_account_key=sa,
        spreadsheet_id=sid.strip(),
        contact_email=email.strip(),
        google_sheets_scopes=scopes,
    )
