#!/usr/bin/env python3
"""Browser smoke for the static site; optional deterministic social-card render."""

from __future__ import annotations

import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parents[1]
SPEC = json.loads((ROOT / "website/pages.json").read_text())
BASE = SPEC["site_url"]


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, *_args):
        pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--render-social", action="store_true")
    parser.add_argument("--live", action="store_true", help="Check the published site, not local output")
    args = parser.parse_args()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        if args.render_social:
            page = browser.new_page(viewport={"width": 1280, "height": 640}, device_scale_factor=1)
            page.goto((ROOT / "website/social-preview.html").as_uri())
            page.locator("img").evaluate("img => img.decode()")
            page.screenshot(path=str(ROOT / "website/assets/social-preview.png"))
            browser.close()
            print("Rendered social-preview.png from tracked HTML/SVG source")
            return
        server = None
        context = browser.new_context(viewport={"width": 1365, "height": 900})
        if not args.live:
            server = ThreadingHTTPServer(("127.0.0.1", 0), partial(QuietHandler, directory=str(ROOT / "build/docs")))
            threading.Thread(target=server.serve_forever, daemon=True).start()
            origin = f"http://127.0.0.1:{server.server_port}/"
            def local(route):
                suffix = route.request.url.removeprefix(BASE)
                response = route.fetch(url=origin + suffix)
                route.fulfill(response=response)
            context.route(BASE + "**", local)
        errors = []
        failed_assets = []
        context.on("response", lambda response: failed_assets.append(response.url)
                   if response.url.startswith(BASE) and response.status >= 400 else None)
        page = context.new_page()
        page.on("pageerror", lambda error: errors.append(str(error)))
        screenshot_dir = ROOT / "build/docs-browser"
        screenshot_dir.mkdir(exist_ok=True)
        for width, label in ((1365, "desktop"), (390, "mobile")):
            page.set_viewport_size({"width": width, "height": 900})
            for route in ("en/", "uk/", "en/compatibility/", "uk/learning/", "en/devices/"):
                response = page.goto(BASE + route, wait_until="networkidle")
                assert response and response.status == 200, route
                assert page.locator("main h1").count() == 1, route
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth + 1"), f"Horizontal overflow: {route} {width}"
                assert page.locator("main img").evaluate_all("imgs => imgs.filter(i => new URL(i.src).origin === location.origin).every(i => i.complete && i.naturalWidth > 0)"), f"Broken image: {route}"
            page.goto(BASE + "en/learning/", wait_until="networkidle")
            page.locator('.language-switch a[hreflang="uk"]').click()
            page.wait_for_url(BASE + "uk/learning/")
            assert page.locator("html").get_attribute("lang") == "uk"
            page.locator('.language-switch a[hreflang="en"]').click()
            page.wait_for_url(BASE + "en/learning/")
            page.goto(BASE + "en/", wait_until="networkidle")
            page.screenshot(path=str(screenshot_dir / f"{label}.png"), full_page=True)
        # Search must work in both languages, not merely ship a JSON file.
        for width in (1365, 390):
            for language in ("en", "uk"):
                page.set_viewport_size({"width": width, "height": 900})
                page.goto(BASE + language + "/", wait_until="networkidle")
                trigger = page.locator('.md-search__button')
                if not trigger.is_visible():
                    trigger = page.locator('.md-header__button[for="__search"]')
                trigger.click()
                query = page.get_by_role("combobox")
                query.fill("SmartESS" if language == "en" else "інвертор")
                result = page.get_by_role("option").first
                result.wait_for(timeout=30000)
                assert result.get_attribute("href").startswith(BASE + language + "/")
                result.click()
                page.wait_for_load_state("networkidle")
                assert page.locator("main h1").count() == 1
        page.emulate_media(color_scheme="dark")
        page.goto(BASE + "uk/", wait_until="networkidle")
        page.screenshot(path=str(screenshot_dir / "mobile-uk-dark.png"), full_page=True)
        assert not errors, errors
        assert not failed_assets, failed_assets
        context.close()
        browser.close()
        if server:
            server.shutdown()
    print("Browser OK: desktop/mobile, images, contextual language switch and EN/UK search")


if __name__ == "__main__":
    main()
