"""Small shared primitives for the stub-based Home Assistant unit tests."""

from __future__ import annotations

import sys
import types


def ensure_module(name: str) -> types.ModuleType:
    """Return an existing module or install one empty test module."""

    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        sys.modules[name] = module
    return module
