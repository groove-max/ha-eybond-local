from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from tools.check_public_docs import (
    ARCHITECTURE_MARKDOWN,
    DOCS_INDEX,
    MAINTAINER_MARKDOWN,
    USER_GUIDE_MARKDOWN,
    _github_slug,
    validate_public_docs,
)


class PublicDocumentationTests(unittest.TestCase):
    def test_public_links_anchors_and_index_are_valid(self) -> None:
        self.assertEqual(validate_public_docs(), ())

    def test_github_slug_preserves_unicode_and_normalizes_spacing(self) -> None:
        self.assertEqual(
            _github_slug("Діагностика та сервісні інструменти"),
            "діагностика-та-сервісні-інструменти",
        )
        self.assertEqual(
            _github_slug("Support Archive vs proxy capture"),
            "support-archive-vs-proxy-capture",
        )

    def test_every_docs_page_has_one_declared_audience(self) -> None:
        user_guides = set(USER_GUIDE_MARKDOWN)
        maintainer_docs = set(MAINTAINER_MARKDOWN)
        architecture_docs = set(ARCHITECTURE_MARKDOWN)

        self.assertFalse(user_guides & maintainer_docs)
        self.assertFalse(user_guides & architecture_docs)
        self.assertFalse(maintainer_docs & architecture_docs)
        self.assertEqual(
            {DOCS_INDEX, *user_guides, *maintainer_docs, *architecture_docs},
            set((REPO_ROOT / "docs").rglob("*.md")),
        )

    def test_docs_root_contains_only_the_audience_index(self) -> None:
        self.assertEqual(
            set((REPO_ROOT / "docs").glob("*.md")),
            {DOCS_INDEX},
        )


if __name__ == "__main__":
    unittest.main()
