#!/usr/bin/env python3
"""Build the allowlisted, bilingual user site from repository sources.

No network or integration imports are needed. The output directory is the ONLY
directory published by CI. Private evidence and maintainer files stay outside it.
"""

from __future__ import annotations

import html
import json
import os
from pathlib import Path
import posixpath
import re
import shutil
import subprocess
import sys
from urllib.parse import quote, unquote, urlsplit, urlunsplit
import xml.etree.ElementTree as ET

import yaml

from docs_catalog import catalog_table, model_page, model_records, model_slug


ROOT = Path(__file__).resolve().parents[1]
SPEC = json.loads((ROOT / "website/pages.json").read_text())
BASE = SPEC["site_url"]
PREFIX = urlsplit(BASE).path
OUTPUT = ROOT / "build/docs"
STAGING = ROOT / "build/docs-source"
FORBIDDEN = {".local", ".git", ".env", "graphify-out", "field_dumps"}
MARKDOWN_LINK = re.compile(r'(!?\[[^\]\n]*\]\()(<[^>]+>|[^\s)]+)([^\n)]*\))')
BADGE_LINK = re.compile(r'(\[!\[[^\]\n]*\]\([^\n)]+\)\]\()([^\s)]+)(\))')
HTML_LINK = re.compile(r'(?P<start>\b(?:href|src)=["\'])(?P<url>[^"\']+)(?P<end>["\'])')
REFERENCE_LINK = re.compile(r'^(\s{0,3}\[[^\]]+\]:\s*)(\S+)(.*)$', re.M)


def source_path(relative: str) -> Path:
    path = (ROOT / relative).resolve()
    if (not path.is_relative_to(ROOT) or FORBIDDEN.intersection(Path(relative).parts)
            or FORBIDDEN.intersection(path.relative_to(ROOT).parts)):
        raise ValueError(f"Forbidden documentation source: {relative}")
    if not path.is_file():
        raise ValueError(f"Missing documentation source: {relative}")
    return path


def routes() -> dict[str, str]:
    result = {}
    for page in SPEC["pages"]:
        for language in SPEC["languages"]:
            if language in page:
                source = page[language]["source"]
                if source in result:
                    raise ValueError(f"Source published twice: {source}")
                result[source] = language + "/" + (page["slug"] + "/" if page["slug"] else "")
    return result


def rewrite_url(value: str, source: str) -> str:
    parsed = urlsplit(html.unescape(value.strip("<>")))
    if parsed.scheme or parsed.netloc or not parsed.path:
        return value
    if parsed.path.startswith("/"):
        if not parsed.path.startswith(PREFIX):
            raise ValueError(f"Root-relative link escapes project: {value}")
        return value
    target = posixpath.normpath(posixpath.join(posixpath.dirname(source), unquote(parsed.path)))
    source_path(target)
    if target in routes():
        destination = PREFIX + routes()[target]
    elif target.startswith("docs/images/") and target.removeprefix("docs/images/") in SPEC["assets"]:
        destination = PREFIX + "assets/images/" + target.removeprefix("docs/images/")
    else:
        # Only reviewed public repository references may stay on GitHub.
        if not (target in {"LICENSE", "CONTRIBUTING.md", "docs/README.md", "tools/README.md"}
                or target.startswith(("docs/maintainer/", "docs/architecture/", "docs/generated/"))):
            raise ValueError(f"Unlisted public link in {source}: {value}")
        destination = SPEC["repository"] + "/blob/main/" + quote(target, safe="/")
    return urlunsplit(("", "", destination, parsed.query, parsed.fragment))


def rewrite_markdown(text: str, source: str) -> str:
    """Rewrite inline/reference Markdown and HTML URLs, but not fenced examples."""
    parts = re.split(r"(^```[^\n]*\n.*?^```[^\n]*$|^~~~[^\n]*\n.*?^~~~[^\n]*$)", text, flags=re.M | re.S)
    for index in range(0, len(parts), 2):
        value = parts[index]
        value = BADGE_LINK.sub(lambda m: m[1] + rewrite_url(m[2], source) + m[3], value)
        value = MARKDOWN_LINK.sub(lambda m: m[1] + rewrite_url(m[2], source) + m[3], value)
        value = HTML_LINK.sub(lambda m: m["start"] + rewrite_url(m["url"], source) + m["end"], value)
        value = REFERENCE_LINK.sub(lambda m: m[1] + rewrite_url(m[2], source) + m[3], value)
        parts[index] = value
    return "".join(parts)


def alternatives(page: dict) -> list[dict]:
    available = [language for language in SPEC["languages"] if language in page]
    suffix = page["slug"] + "/" if page["slug"] else ""
    result = [{"lang": language, "name": SPEC["languages"][language],
               "url": BASE + language + "/" + suffix} for language in available]
    if len(available) > 1:
        result.append({"lang": "x-default", "name": "English", "url": BASE + "en/" + suffix})
    return result


def stage(page: dict, language: str, text: str) -> dict:
    item = page[language]
    relative = page["slug"] + "/index.md" if page["slug"] else "index.md"
    path = STAGING / language / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    meta = {"title": item["title"], "description": item["description"],
            "locale": "uk_UA" if language == "uk" else "en_US",
            "alternates": alternatives(page), "english_only": "uk" not in page}
    path.write_text("---\n" + yaml.safe_dump(meta, allow_unicode=True, sort_keys=False) + "---\n\n" + text)
    return {"url": BASE + language + "/" + (page["slug"] + "/" if page["slug"] else ""),
            "language": language, "file": language + "/" + relative.replace(".md", ".html"),
            **meta}


def write_root(token: str) -> None:
    verify = f'<meta name="google-site-verification" content="{html.escape(token)}">' if token else ""
    (OUTPUT / "index.html").write_text(
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>EyeBond Local — Home Assistant inverter integration</title>'
        '<meta name="description" content="Local inverter monitoring and supported controls with EyeBond Local. English and Ukrainian documentation.">'
        f'<link rel="canonical" href="{BASE}en/">'
        '<meta property="og:type" content="website"><meta property="og:title" content="EyeBond Local">'
        '<meta property="og:description" content="Local inverter monitoring and supported controls for Home Assistant.">'
        f'<meta property="og:url" content="{BASE}en/">'
        f'<meta property="og:image" content="{BASE}assets/social-preview.png">'
        '<meta name="twitter:card" content="summary_large_image">'
        + verify + '<meta http-equiv="refresh" content="0;url=en/"></head>'
        '<body><main><h1>EyeBond Local</h1><p><a href="en/">English</a> · '
        '<a href="uk/" lang="uk">Українська</a></p></main></body></html>'
    )
    (OUTPUT / "404.html").write_text(
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta name="robots" content="noindex"><title>Page not found — EyeBond Local</title>'
        '</head><body><main><h1>404 — Page not found / Сторінку не знайдено</h1>'
        f'<p><a href="{PREFIX}en/">English</a> · <a href="{PREFIX}uk/">Українська</a></p>'
        '</main></body></html>'
    )


def main() -> None:
    token = os.environ.get("GOOGLE_SITE_VERIFICATION", "")
    if token and not re.fullmatch(r"[A-Za-z0-9_=-]+", token):
        raise ValueError("Invalid site verification token")
    if not BASE.startswith("https://") or not BASE.endswith("/"):
        raise ValueError("Expected canonical HTTPS base ending in slash")
    routes()  # Validate uniqueness before touching build output.
    for path in (OUTPUT, STAGING):
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True)
    (STAGING / "overrides").mkdir()
    shutil.copy2(source_path("website/overrides/main.html"), STAGING / "overrides/main.html")
    assets = OUTPUT / "assets"
    (assets / "images").mkdir(parents=True)
    for name in SPEC["assets"]:
        if Path(name).name != name:
            raise ValueError("Invalid image name")
        shutil.copy2(source_path("docs/images/" + name), assets / "images" / name)
    shutil.copy2(source_path("website/assets/social-preview.png"), assets / "social-preview.png")
    shutil.copy2(source_path("website/assets/ZENSICAL-LICENSE.txt"), assets / "ZENSICAL-LICENSE.txt")
    records = model_records(ROOT)
    pages = list(SPEC["pages"])
    generated = {}
    # Full reference stays intact for old deep links; model pages add readable entries.
    for record in records:
        name = f"{record['manufacturer']} {record['model']}"
        slug = model_slug(record)
        pages.append({"slug": slug, "section": "models", "en": {
            "label": name, "title": name + " Home Assistant compatibility",
            "description": f"{name}: recorded local readings, control support, firmware variants and limitations with EyeBond Local. Check the evidence before setup.",
        }})
        generated[slug] = model_page(record, ROOT, BASE)
    pages.append({"slug": "devices", "section": "models", "en": {
        "label": "Model catalog", "title": "EyeBond inverter model catalog for Home Assistant",
        "description": "Browse reviewed inverter models, confirmed readings, control support and known limitations. Anenji, PowMr, MUST, Kevolt and other EyeBond devices.",
    }})
    generated["devices"] = (
        "# Inverter model catalog\n\nEach page describes a recorded variant, not every device from a brand. "
        "The catalog follows current main; some support is newer than published beta.5. "
        f"[How to read compatibility]({BASE}en/compatibility/).\n\n" + catalog_table(records, BASE, "en")
    )
    built = []
    for language in SPEC["languages"]:
        nav, references, models = [], [], []
        for page in pages:
            if language not in page:
                continue
            item = page[language]
            if "source" in item:
                text = rewrite_markdown(source_path(item["source"]).read_text(), item["source"])
                # The template supplies contextual language links.
                text = re.sub(r"^\[(?:Українська|English)\]\([^\n]+\)\s*$", "", text, flags=re.M)
            else:
                text = generated[page["slug"]]
            if page["slug"] == "compatibility":
                text += "\n" + catalog_table(records, BASE, language)
            built.append(stage(page, language, text))
            link = page["slug"] + "/index.md" if page["slug"] else "index.md"
            target = references if page.get("section") == "reference" else models if page.get("section") == "models" else nav
            target.append({item["label"]: link})
        if models:
            nav.append({"Model catalog": [{"All models": "devices/index.md"}] + [m for m in models if "Model catalog" not in m]})
        if references:
            nav.append({"Detailed reference": references})
        if language == "uk":
            nav += [{"Каталог моделей (English)": BASE + "en/devices/"}, {"Довідники (English)": BASE + "en/reference/runtime/"}]
        lang_assets = STAGING / language / "assets"
        lang_assets.mkdir()
        for name in ("eybond.svg", "site.css"):
            shutil.copy2(source_path("website/assets/" + name), lang_assets / name)
        config = yaml.safe_load((ROOT / "website/site.yml").read_text())
        config.update({"docs_dir": "docs-source/" + language, "site_dir": "docs/" + language,
                       "site_url": BASE + language + "/", "nav": nav})
        config["theme"].update({"language": language, "custom_dir": "docs-source/overrides"})
        config["extra"].update({"public_url": BASE, "google_site_verification": token,
            "version_notice": "0.3 development docs · main fixes may be newer than published beta.5" if language == "en"
            else "Документація розробки 0.3 · main може містити новіші виправлення, ніж beta.5"})
        # Match GitHub heading fragments, including Ukrainian and underscores.
        for extension in config["markdown_extensions"]:
            if isinstance(extension, dict) and "toc" in extension:
                extension["toc"]["slugify"] = {"object": "pymdownx.slugs.slugify", "kwds": {"case": "lower", "percent_encode": False}}
        config_path = ROOT / "build" / f"docs-{language}.yml"
        config_path.write_text(yaml.safe_dump(config, allow_unicode=True, sort_keys=False))
        subprocess.run([sys.executable, "-m", "zensical", "build", "-f", str(config_path), "--clean", "--strict"], cwd=ROOT, check=True)
        # Zensical generates an independent sitemap for each language; publish one
        # explicit canonical list below, with alternates only for real translations.
        for generated_name in ("sitemap.xml", "sitemap.xml.gz"):
            (OUTPUT / language / generated_name).unlink(missing_ok=True)
    write_root(token)
    ns, xhtml = "http://www.sitemaps.org/schemas/sitemap/0.9", "http://www.w3.org/1999/xhtml"
    ET.register_namespace("", ns)
    ET.register_namespace("xhtml", xhtml)
    sitemap = ET.Element(f"{{{ns}}}urlset")
    for page in built:
        entry = ET.SubElement(sitemap, f"{{{ns}}}url")
        ET.SubElement(entry, f"{{{ns}}}loc").text = page["url"]
        for alternate in page["alternates"]:
            ET.SubElement(entry, f"{{{xhtml}}}link", {"rel": "alternate", "hreflang": alternate["lang"], "href": alternate["url"]})
    ET.ElementTree(sitemap).write(OUTPUT / "sitemap.xml", encoding="utf-8", xml_declaration=True)
    (OUTPUT / ".nojekyll").touch()
    (ROOT / "build/docs-manifest.json").write_text(json.dumps(built, ensure_ascii=False, indent=2))
    print(f"Built {len(built)} canonical pages; only build/docs is public.")


if __name__ == "__main__":
    main()
