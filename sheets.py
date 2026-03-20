from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone

from google.oauth2 import service_account
from googleapiclient.discovery import build

HEADERS = [
    "Titular",
    "Categoría",
    "Autor",
    "Tiempo de lectura",
    "Fecha de publicación",
]

# Google Sheet tab titles cannot contain: \ / * ? : [ ]
_ILLEGAL_IN_TITLE = re.compile(r'[\\/*?:\[\]]')


def _sanitize_sheet_title_base(label: str) -> str:
    s = "".join(
        c
        for c in unicodedata.normalize("NFD", label)
        if unicodedata.category(c) != "Mn"
    )
    s = _ILLEGAL_IN_TITLE.sub("_", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[^\w\s\-]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip(" _")
    return (s[:50] if s else "Blog").strip()


def _a1_range(sheet_name: str, cell: str) -> str:
    if re.match(r"^[A-Za-z0-9_]+$", sheet_name):
        return f"{sheet_name}!{cell}"
    safe = sheet_name.replace("'", "''")
    return f"'{safe}'!{cell}"


def _list_sheet_titles(svc, spreadsheet_id: str) -> set[str]:
    meta = (
        svc.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title")
        .execute()
    )
    return {
        s["properties"]["title"]
        for s in meta.get("sheets", [])
        if s.get("properties", {}).get("title")
    }


def _pick_new_sheet_title(category_label: str, existing: set[str]) -> str:
    base = _sanitize_sheet_title_base(category_label)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    name = f"{base}_{ts}"[:100]
    if name not in existing:
        return name
    n = 1
    while True:
        suffix = f"_{n}"
        stem = name[: 100 - len(suffix)]
        candidate = f"{stem}{suffix}"
        if candidate not in existing:
            return candidate
        n += 1


def create_sheet_and_write_blog_posts(
    settings,
    rows: list[dict[str, str]],
    *,
    category_label: str,
) -> tuple[str, str]:
    """
    Adds a new tab to the spreadsheet, writes headers + rows.
    Returns (sheet_title, url_opening_that_tab).
    """
    creds = service_account.Credentials.from_service_account_info(
        settings.google_service_account_key,
        scopes=list(settings.google_sheets_scopes),
    )
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    sid = settings.spreadsheet_id

    existing = _list_sheet_titles(svc, sid)
    title = _pick_new_sheet_title(category_label, existing)

    add_resp = (
        svc.spreadsheets()
        .batchUpdate(
            spreadsheetId=sid,
            body={
                "requests": [
                    {"addSheet": {"properties": {"title": title}}},
                ]
            },
        )
        .execute()
    )
    sheet_id = int(
        add_resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    )

    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=_a1_range(title, "A1:E1"),
        valueInputOption="USER_ENTERED",
        body={"values": [HEADERS]},
    ).execute()

    if rows:
        body_rows = [
            [
                r.get("Titular", ""),
                r.get("Categoría", ""),
                r.get("Autor", ""),
                r.get("Tiempo de lectura", ""),
                r.get("Fecha de publicación", ""),
            ]
            for r in rows
        ]
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=_a1_range(title, "A2"),
            valueInputOption="USER_ENTERED",
            body={"values": body_rows},
        ).execute()

    link = f"https://docs.google.com/spreadsheets/d/{sid}/edit?gid={sheet_id}"
    return title, link
