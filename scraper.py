"""
Blog post collection: parse category pages with BeautifulSoup when links exist,
otherwise resolve posts via Sanity's public API (same source as xepelin.com).
Article pages are fetched with requests and parsed for reading time and metadata.
"""

from __future__ import annotations

import os
import re
import unicodedata
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 45

# Normalized key (accents stripped, lower, single spaces) -> (canonical label, URL path segment)
_CATEGORY_SLUGS: dict[str, tuple[str, str]] = {
    "pymes": ("Pymes", "pymes"),
    "corporativos": ("Corporativos", "corporativos"),
    "educacion financiera": ("Educación Financiera", "educacion-financiera"),
    "emprendedores": ("Emprendedores", "emprendedores"),
    "xepelin": ("Xepelin", "noticias"),
    "casos de exito": ("Casos de éxito", "empresarios-exitosos"),
}

ALLOWED_CATEGORIES_TEXT = ", ".join(t[0] for t in _CATEGORY_SLUGS.values())


def normalize_category_key(raw: str) -> str:
    s = raw.strip()
    s = "".join(
        c
        for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )
    return " ".join(s.lower().split())


def resolve_category(raw: str) -> tuple[str, str]:
    """Returns (canonical_display_name, url_path_slug)."""
    key = normalize_category_key(raw)
    if key not in _CATEGORY_SLUGS:
        raise ValueError(
            f"Unknown category {raw!r}. Allowed: {ALLOWED_CATEGORIES_TEXT}"
        )
    return _CATEGORY_SLUGS[key]


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "es-CL,es;q=0.9"})
    return s


def _category_listing_url(base: str, path_slug: str) -> str:
    return f"{base.rstrip('/')}/{path_slug.strip('/')}"


def urls_from_listing_html(html: str, page_url: str, path_slug: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    host = urlparse(page_url).netloc
    prefix = f"/blog/{path_slug}/"
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        full = urljoin(page_url + "/", href)
        p = urlparse(full)
        if p.netloc and p.netloc != host:
            continue
        path = p.path.rstrip("/")
        if not path.startswith(prefix.rstrip("/")):
            continue
        rest = path[len(prefix.rstrip("/")) :].lstrip("/")
        if not rest or "/" in rest:
            continue
        out.append(f"{p.scheme or 'https'}://{p.netloc or host}{prefix}{rest}")
    return list(dict.fromkeys(out))


def sanity_fetch_posts(
    project_id: str, dataset: str, category_slug: str
) -> list[dict[str, Any]]:
    """blogCategory.slug.current matches the URL segment (e.g. pymes, noticias)."""
    query = """
    *[_type == "blogArticle"
      && references(*[_type == "blogCategory" && slug.current == $slug]._id)]
      | order(date desc) [0...500] {
        "slug": slug.current,
        title,
        date,
        "authorName": author->name,
        "categoryTitle": *[_type == "blogCategory" && slug.current == $slug][0].title
      }
    """
    url = f"https://{project_id}.api.sanity.io/v2021-06-07/data/query/{dataset}"
    r = requests.get(
        url,
        params=[("query", query.strip()), ("$slug", f'"{category_slug}"')],
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"].get("description", data["error"]))
    return list(data.get("result") or [])


def discover_posts(
    blog_base_url: str,
    path_slug: str,
    sanity_project_id: str,
    sanity_dataset: str,
) -> list[dict[str, Any]]:
    """
    Each item: url, title, author, category_label, date_iso (optional YYYY-MM-DD).
    """
    listing_url = _category_listing_url(blog_base_url, path_slug)
    sess = _session()
    r = sess.get(listing_url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    urls = urls_from_listing_html(r.text, listing_url, path_slug)

    posts: list[dict[str, Any]] = []
    if urls:
        for u in urls:
            posts.append(
                {
                    "url": u,
                    "title": None,
                    "author": None,
                    "category_label": None,
                    "date_iso": None,
                }
            )
        return posts

    rows = sanity_fetch_posts(sanity_project_id, sanity_dataset, path_slug)
    base = blog_base_url.rstrip("/")
    for row in rows:
        slug = row.get("slug")
        if not slug:
            continue
        url = f"{base}/{path_slug}/{slug}"
        d = row.get("date")
        date_iso = None
        if isinstance(d, str) and len(d) >= 10:
            date_iso = d[:10]
        posts.append(
            {
                "url": url,
                "title": row.get("title"),
                "author": row.get("authorName"),
                "category_label": row.get("categoryTitle"),
                "date_iso": date_iso,
            }
        )
    return posts


_READING_TIME_RE = re.compile(
    r"(\d+)\s*(?:<!--\s*-->\s*)?min\s+de\s+lectura",
    re.IGNORECASE,
)


def _meta_content(soup: BeautifulSoup, *, prop: str | None = None, name: str | None = None) -> str | None:
    if prop:
        tag = soup.find("meta", property=prop)
        if tag and tag.get("content"):
            return tag["content"].strip()
    if name:
        tag = soup.find("meta", attrs={"name": name})
        if tag and tag.get("content"):
            return tag["content"].strip()
    return None


def _json_ld_date_published(html: str) -> str | None:
    for m in re.finditer(
        r'<script[^>]+type="application/ld\+json"[^>]*>([^<]+)</script>',
        html,
        re.I,
    ):
        chunk = m.group(1)
        if "datePublished" not in chunk:
            continue
        m2 = re.search(
            r'"datePublished"\s*:\s*"([^"]+)"',
            chunk,
        )
        if m2:
            return m2.group(1)[:10]
    return None


def scrape_article_page(
    sess: requests.Session,
    url: str,
    *,
    fallback_category: str,
    preset_title: str | None = None,
    preset_author: str | None = None,
    preset_category: str | None = None,
    preset_date_iso: str | None = None,
) -> dict[str, str]:
    r = sess.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    title = preset_title
    if not title:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(" ", strip=True)
    if not title:
        title = _meta_content(soup, prop="og:title") or ""

    author = preset_author or _meta_content(soup, name="author")
    if not author:
        # Common byline: first strong text after h1 (site-dependent fallback)
        pass

    category = preset_category or fallback_category
    if not preset_category:
        # Try og:article:section
        sec = _meta_content(soup, prop="article:section")
        if sec:
            category = sec

    date_iso = preset_date_iso
    if not date_iso:
        pub = _meta_content(soup, prop="article:published_time")
        if pub and len(pub) >= 10:
            date_iso = pub[:10]
    if not date_iso:
        date_iso = _json_ld_date_published(html)

    raw_html_for_rt = html.replace("<!-- -->", "")
    rm = _READING_TIME_RE.search(raw_html_for_rt)
    reading = ""
    if rm:
        reading = f"{rm.group(1)} min"

    return {
        "Titular": title.strip(),
        "Categoría": (category or fallback_category).strip(),
        "Autor": (author or "").strip(),
        "Tiempo de lectura": reading,
        "Fecha de publicación": (date_iso or "").strip(),
    }


def scrape_category(
    blog_base_url: str,
    path_slug: str,
    canonical_category: str,
    sanity_project_id: str,
    sanity_dataset: str,
) -> list[dict[str, str]]:
    posts = discover_posts(
        blog_base_url, path_slug, sanity_project_id, sanity_dataset
    )
    cap = os.environ.get("SCRAPE_MAX_POSTS", "").strip()
    if cap.isdigit():
        posts = posts[: int(cap)]
    if not posts:
        return []

    sess = _session()
    rows: list[dict[str, str]] = []
    for p in posts:
        try:
            row = scrape_article_page(
                sess,
                p["url"],
                fallback_category=canonical_category,
                preset_title=p.get("title"),
                preset_author=p.get("author"),
                preset_category=p.get("category_label"),
                preset_date_iso=p.get("date_iso"),
            )
            if row["Titular"]:
                rows.append(row)
        except requests.RequestException:
            continue
    return rows
