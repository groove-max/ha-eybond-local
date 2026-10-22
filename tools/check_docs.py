#!/usr/bin/env python3
"""Validate the actual generated HTML, not merely the intended configuration."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import struct
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "build/docs"
SPEC = json.loads((ROOT / "website/pages.json").read_text())
BASE = SPEC["site_url"]
PREFIX = urlsplit(BASE).path
FORBIDDEN = {".local", ".git", "graphify-out", "field_dumps", "maintainer", "architecture"}


def check_site() -> list[str]:
    errors = []
    pages = json.loads((ROOT / "build/docs-manifest.json").read_text())
    documents = {}
    def check(condition, message):
        if not condition:
            errors.append(message)
    def document(path):
        if path not in documents:
            documents[path] = BeautifulSoup(path.read_text(), "html.parser")
        return documents[path]
    def local_target(value, current):
        parsed = urlsplit(value)
        if parsed.scheme and parsed.scheme not in {"https", "http"}:
            return None
        if parsed.netloc and parsed.netloc != urlsplit(BASE).netloc:
            return None
        part = unquote(parsed.path)
        if part.startswith(PREFIX):
            path = OUTPUT / part.removeprefix(PREFIX)
        elif part.startswith("/"):
            check(False, f"Project-prefix escape: {value}")
            return None
        else:
            path = current.parent / part if part else current
        path = path.resolve()
        if not path.is_relative_to(OUTPUT.resolve()):
            check(False, f"Output escape: {value}")
            return None
        if part.endswith("/") or path.is_dir():
            path /= "index.html"
        return path, unquote(parsed.fragment)
    expected = {page["url"] for page in pages}
    check(len(expected) == len(pages), "Duplicate canonical routes")
    titles, descriptions = set(), set()
    for page in pages:
        path = OUTPUT / page["file"]
        check(path.is_file(), f"Missing page {path}")
        if not path.is_file():
            continue
        soup = document(path)
        url = page["url"]
        check(soup.html and soup.html.get("lang") == page["language"], f"Wrong language: {url}")
        check(len(soup.select("main h1")) == 1, f"Expected one H1: {url}")
        check(soup.title and page["title"] in soup.title.text, f"Wrong title: {url}")
        check(page["title"] not in titles, f"Duplicate title: {url}")
        check(page["description"] not in descriptions, f"Duplicate description: {url}")
        titles.add(page["title"])
        descriptions.add(page["description"])
        canonicals = soup.select('link[rel="canonical"]')
        check(len(canonicals) == 1 and canonicals[0].get("href") == url, f"Wrong canonical: {url}")
        for selector, value in {
            'meta[name="description"]': page["description"],
            'meta[property="og:title"]': page["title"],
            'meta[property="og:description"]': page["description"],
            'meta[property="og:url"]': url,
            'meta[property="og:image"]': BASE + "assets/social-preview.png",
            'meta[property="og:locale"]': page["locale"],
            'meta[name="twitter:card"]': "summary_large_image",
        }.items():
            tags = soup.select(selector)
            check(len(tags) == 1 and tags[0].get("content") == value, f"Wrong {selector}: {url}")
        check(not soup.select('meta[name="robots"][content*="noindex"]'), f"Content is noindex: {url}")
        # Sitemap is the single search-engine hreflang implementation. Head
        # alternates trigger the theme's site-root language redirect handler.
        check(not soup.select('head link[rel="alternate"][hreflang]'), f"Conflicting language handler: {url}")
        for alt in page["alternates"]:
            check(alt["url"] in expected, f"Alternate not published: {alt}")
            if alt["lang"] == "x-default":
                continue
            links = soup.select(f'.language-switch a[hreflang="{alt["lang"]}"]')
            check(len(links) == 1 and links[0].get("href") == alt["url"], f"Switch loses page: {url}")
        ids = [node["id"] for node in soup.select("[id]")]
        check(len(ids) == len(set(ids)), f"Duplicate HTML ids: {url}")
        for node in soup.select("main img"):
            check(bool(node.get("alt")), f"Image without description: {url}")
        if page["url"] in {BASE + "en/", BASE + "uk/"}:
            check(bool(soup.select('a[href*="my.home-assistant.io/redirect/hacs_repository/"] img')), f"Missing install badge: {url}")
    # Validate the root and 404 pages as well as all content, assets and fragments.
    for path in sorted(OUTPUT.rglob("*.html")):
        soup = document(path)
        for node in soup.select("[href], [src], meta[property='og:image']"):
            value = node.get("href") or node.get("src") or node.get("content")
            if not value:
                continue
            target = local_target(value, path)
            if target is None:
                continue
            file, fragment = target
            check(file.is_file(), f"Broken link in {path.relative_to(OUTPUT)}: {value}")
            if fragment and file.is_file() and file.suffix == ".html":
                check(document(file).find(id=fragment) is not None, f"Broken anchor in {path.relative_to(OUTPUT)}: {value}")
        token = os.environ.get("GOOGLE_SITE_VERIFICATION", "")
        if path.name != "404.html":
            tag = soup.select_one('meta[name="google-site-verification"]')
            check(tag and tag.get("content") == token if token else tag is None, f"Wrong verification tag: {path}")
    sitemap = ET.parse(OUTPUT / "sitemap.xml")
    entries = sitemap.findall("./{*}url")
    actual_urls = [item.find("{*}loc").text for item in entries]
    check(len(actual_urls) == len(expected) and set(actual_urls) == expected, "Sitemap differs from canonical pages")
    by_url = {page["url"]: page for page in pages}
    for entry in entries:
        url = entry.find("{*}loc").text
        if url not in by_url:
            continue
        actual = {link.get("hreflang"): link.get("href") for link in entry.findall("{http://www.w3.org/1999/xhtml}link")}
        wanted = {alt["lang"]: alt["url"] for alt in by_url[url]["alternates"]}
        check(actual == wanted, f"Sitemap language mismatch: {url}")
    for path in OUTPUT.rglob("*"):
        relative = path.relative_to(OUTPUT)
        check(not path.is_symlink(), f"Symlink in output: {relative}")
        check(not FORBIDDEN.intersection(relative.parts), f"Private/non-user directory: {relative}")
        check(path.suffix not in {".py", ".zip", ".env", ".log", ".jsonl", ".sqlite", ".yaml", ".yml", ".md"}, f"Unexpected source/evidence: {relative}")
    png = (OUTPUT / "assets/social-preview.png").read_bytes()
    check(png[:8] == b"\x89PNG\r\n\x1a\n" and struct.unpack(">II", png[16:24]) == (1280, 640), "Wrong social-card dimensions")
    return list(dict.fromkeys(errors))


def main() -> None:
    errors = check_site()
    if errors:
        raise SystemExit("\n".join(errors))
    print("Site OK: canonical pages, links, anchors, images, languages, sitemap, verification and public-only output")


if __name__ == "__main__":
    main()
