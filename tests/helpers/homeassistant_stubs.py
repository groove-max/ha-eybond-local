"""Small shared primitives for the stub-based Home Assistant unit tests."""

from __future__ import annotations

import sys
import types
from pathlib import Path


def ensure_module(name: str) -> types.ModuleType:
    """Return an existing module or install one empty test module."""

    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        sys.modules[name] = module
    return module


def ensure_package(name: str, path: Path) -> types.ModuleType:
    """Return a test module that still permits importing real child modules."""

    module = ensure_module(name)
    package_path = str(path)
    paths = list(getattr(module, "__path__", ()))
    if package_path not in paths:
        paths.append(package_path)
    module.__path__ = paths
    return module
