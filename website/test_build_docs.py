"""Tests for publication boundaries and source-to-site routing (no device I/O)."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import build_docs
import docs_catalog


class DocumentationBuildTests(unittest.TestCase):
    def test_all_sources_and_images_are_present_and_unique(self):
        routes = build_docs.routes()
        self.assertEqual(len(routes), len(set(routes.values())))
        for source in routes:
            build_docs.source_path(source)
            self.assertTrue(source.startswith("docs/user/") or source in {
                "README.md", "README.uk.md", "docs/generated/INVERTER_MODEL_CATALOG.generated.md",
            })
        for name in build_docs.SPEC["assets"]:
            build_docs.source_path("docs/images/" + name)

    def test_rewrites_markdown_html_badges_and_references(self):
        text = '''[Setup](docs/user/GETTING_STARTED.md#check-readings-before-controls)
<img src="docs/images/device-overview.png" alt="Example">
[![License](https://example.com/badge.svg)](LICENSE)
[guide]: docs/user/INSTALLATION.uk.md
'''
        result = build_docs.rewrite_markdown(text, "README.md")
        self.assertIn("/ha-eybond-local/en/getting-started/#check-readings-before-controls", result)
        self.assertIn('src="/ha-eybond-local/assets/images/device-overview.png"', result)
        self.assertIn("https://github.com/groove-max/ha-eybond-local/blob/main/LICENSE", result)
        self.assertIn("[guide]: /ha-eybond-local/uk/installation/", result)

    def test_fenced_examples_are_not_rewritten(self):
        example = '```markdown\n[x](.local/private.md)\n```\n'
        self.assertEqual(build_docs.rewrite_markdown(example, "README.md"), example)

    def test_rejects_private_and_unlisted_sources(self):
        for value in (".local/WORKFLOW.md", "../outside", "graphify-out/graph.json", "field_dumps/file.txt"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                build_docs.rewrite_url(value, "README.md")
        with self.assertRaises(ValueError):
            build_docs.rewrite_url("custom_components/eybond_local/manifest.json", "README.md")
        with self.assertRaises(ValueError):
            build_docs.rewrite_url("/another-project/file", "README.md")

    def test_real_translations_have_reciprocal_page_specific_alternates(self):
        for page in build_docs.SPEC["pages"]:
            alternatives = build_docs.alternatives(page)
            self.assertEqual({a["lang"] for a in alternatives}, {"en", "uk", "x-default"} if "uk" in page else {"en"})
            for alt in alternatives:
                self.assertTrue(alt["url"].endswith(page["slug"] + "/"))

    def test_model_pages_preserve_limits_and_exclude_private_sources(self):
        records = docs_catalog.model_records(ROOT)
        self.assertEqual(len(records), len({r["model_key"] for r in records}))
        for record in records:
            text = docs_catalog.model_page(record, ROOT, build_docs.BASE)
            self.assertIn(record["knowledge_summary"], text)
            for limitation in record.get("known_limitations", []):
                self.assertIn(limitation, text)
            self.assertNotIn("field_dumps/", text)
            self.assertNotIn(".local/", text)

    def test_private_source_reference_is_not_published(self):
        record = {"manufacturer": "Example", "model": "Test", "knowledge_summary": "Evidence pending.",
                  "lifecycle": "research", "validation": {"telemetry": "unknown", "controls": "none", "hardware": "reported"},
                  "variants": [], "source_keys": ["private"]}
        source = {"visibility": "private", "summary_publishable": True, "reference": "https://private.example/secret"}
        with patch.object(Path, "read_text", return_value=json.dumps(source)):
            text = docs_catalog.model_page(record, ROOT, build_docs.BASE)
        self.assertNotIn("private.example", text)
        self.assertIn("not yet established", text)


if __name__ == "__main__":
    unittest.main()
