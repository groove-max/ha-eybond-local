"""Collector-entry options-flow composition root."""

from __future__ import annotations

from .options_base import OptionsFlowBase
from .options_diagnostics import DiagnosticsOptionsMixin
from .options_proxy import ProxyCaptureOptionsMixin
from .options_runtime import RuntimeOptionsMixin
from .options_shadow_review import ShadowLearningReviewMixin
from .options_shadow_run import ShadowLearningRunMixin
from .options_shadow_runtime import ShadowLearningRuntimeMixin
from .options_strategy import StrategyTransitionOptionsMixin


class EybondLocalOptionsFlow(
    StrategyTransitionOptionsMixin,
    RuntimeOptionsMixin,
    ShadowLearningRunMixin,
    ShadowLearningReviewMixin,
    ShadowLearningRuntimeMixin,
    ProxyCaptureOptionsMixin,
    DiagnosticsOptionsMixin,
    OptionsFlowBase,
):
    """Home Assistant options flow assembled from cohesive lifecycle mixins."""


__all__ = ["EybondLocalOptionsFlow"]
