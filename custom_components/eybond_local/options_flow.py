"""Collector-entry options-flow composition root."""

from __future__ import annotations

from .flows.options.base import OptionsFlowBase
from .flows.options.diagnostics import DiagnosticsOptionsMixin
from .flows.options.local_register_observation import LocalRegisterObservationOptionsMixin
from .flows.options.proxy import ProxyCaptureOptionsMixin
from .flows.options.runtime import RuntimeOptionsMixin
from .flows.options.shadow_review import ShadowLearningReviewMixin
from .flows.options.shadow_run import ShadowLearningRunMixin
from .flows.options.shadow_runtime import ShadowLearningRuntimeMixin
from .flows.options.strategy import StrategyTransitionOptionsMixin


class EybondLocalOptionsFlow(
    StrategyTransitionOptionsMixin,
    RuntimeOptionsMixin,
    LocalRegisterObservationOptionsMixin,
    ShadowLearningRunMixin,
    ShadowLearningReviewMixin,
    ShadowLearningRuntimeMixin,
    ProxyCaptureOptionsMixin,
    DiagnosticsOptionsMixin,
    OptionsFlowBase,
):
    """Home Assistant options flow assembled from cohesive lifecycle mixins."""


__all__ = ["EybondLocalOptionsFlow"]
