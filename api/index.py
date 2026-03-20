import logging

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl

from config import load_settings
from scraper import resolve_category, scrape_category
from sheets import create_sheet_and_write_blog_posts

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(title="Xepelin blog scraper", version="1.0.0")


class ScrapeRequest(BaseModel):
    category: str
    webhook_url: HttpUrl


@app.post("/scrape")
def scrape(req: ScrapeRequest) -> dict:
    try:
        canonical, path_slug = resolve_category(req.category)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    try:
        settings = load_settings()
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    rows = scrape_category(
        settings.blog_base_url,
        path_slug,
        canonical,
        settings.sanity_project_id,
        settings.sanity_dataset,
    )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No se encontraron artículos o no se pudo leer ninguna página.",
        )

    try:
        sheet_title, link = create_sheet_and_write_blog_posts(
            settings, rows, category_label=canonical
        )
    except Exception as e:
        log.exception("Google Sheets error")
        raise HTTPException(status_code=500, detail=f"Sheets error: {e}") from e

    payload = {"email": settings.contact_email, "link": link}
    try:
        wh = requests.post(
            str(req.webhook_url),
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=45,
        )
        wh.raise_for_status()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Webhook failed: {e}") from e

    return {
        "ok": True,
        "rows_written": len(rows),
        "sheet_title": sheet_title,
        "link": link,
    }


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
