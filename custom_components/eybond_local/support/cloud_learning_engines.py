"""Registry binding cloud API adapters to product learning methods."""

from __future__ import annotations

from abc import ABC, abstractmethod

from .cloud_api_adapters import (
    CloudApiAdapter,
    DessMonitorCloudApiAdapter,
    SmartEssCloudApiAdapter,
    UnavailableCloudApiAdapter,
    ValueCloudApiAdapter,
)
from .cloud_control_discovery import (
    SmartEssActiveCorrelationOperation,
    UnavailableCloudLearningRunner,
    ValueCloudActiveCorrelationOperation,
)
from .cloud_active_workflow import ActiveCorrelationWorkflowRunner
from .cloud_learning_models import (
    ACTIVE_CORRELATION_METHOD,
    LOCAL_SERIES_EVIDENCE,
    NO_LOCAL_EVIDENCE,
    READ_ONLY_EVIDENCE_METHOD,
    CloudApiSource,
    CloudLearningEvidenceCapabilities,
    CloudLearningMethod,
    CloudLearningSelection,
    source_supports_method,
)
from .cloud_learning_runner import CloudLearningRunner
from .cloud_read_only_workflow import ReadOnlyEvidenceWorkflowRunner
from .dessmonitor_active import DessMonitorActiveCorrelationOperation
from .dessmonitor_learning import DessMonitorReadOnlyEvidenceOperation
from .smartess_read_only import SmartEssReadOnlyEvidenceOperation


class CloudLearningEngine(ABC):
    """One registered API-source and product-method implementation."""

    adapter: CloudApiAdapter
    method: CloudLearningMethod | None
    evidence_capabilities: CloudLearningEvidenceCapabilities = NO_LOCAL_EVIDENCE
    default_for_method: bool = False

    @property
    def available(self) -> bool:
        return True

    @property
    def source(self) -> CloudApiSource:
        return self.adapter.source

    @property
    def selection(self) -> CloudLearningSelection | None:
        if type(self.method) is not CloudLearningMethod:
            return None
        return CloudLearningSelection(
            method_id=self.method.method_id,
            source_id=self.source.source_id,
        )

    @abstractmethod
    def learning_runner(self) -> CloudLearningRunner:
        """Return a fresh provider-owned runner for one transient flow run."""

    def classify_error(self, exc: BaseException) -> str:
        return self.adapter.classify_error(exc)


class SmartEssCloudLearningEngine(CloudLearningEngine):
    adapter = SmartEssCloudApiAdapter()
    method = ACTIVE_CORRELATION_METHOD
    default_for_method = True

    def learning_runner(self) -> CloudLearningRunner:
        return ActiveCorrelationWorkflowRunner(
            SmartEssActiveCorrelationOperation()
        )


class ValueCloudCloudLearningEngine(CloudLearningEngine):
    adapter = ValueCloudApiAdapter()
    method = ACTIVE_CORRELATION_METHOD
    default_for_method = True

    def learning_runner(self) -> CloudLearningRunner:
        return ActiveCorrelationWorkflowRunner(
            ValueCloudActiveCorrelationOperation()
        )


class DessMonitorCloudLearningEngine(CloudLearningEngine):
    adapter = DessMonitorCloudApiAdapter()
    method = READ_ONLY_EVIDENCE_METHOD
    evidence_capabilities = LOCAL_SERIES_EVIDENCE
    default_for_method = True

    def learning_runner(self) -> CloudLearningRunner:
        return ReadOnlyEvidenceWorkflowRunner(
            DessMonitorReadOnlyEvidenceOperation()
        )


class DessMonitorActiveCloudLearningEngine(CloudLearningEngine):
    adapter = DessMonitorCloudApiAdapter()
    method = ACTIVE_CORRELATION_METHOD

    def learning_runner(self) -> CloudLearningRunner:
        return ActiveCorrelationWorkflowRunner(
            DessMonitorActiveCorrelationOperation()
        )


class SmartEssReadOnlyCloudLearningEngine(CloudLearningEngine):
    adapter = SmartEssCloudApiAdapter()
    method = READ_ONLY_EVIDENCE_METHOD
    evidence_capabilities = LOCAL_SERIES_EVIDENCE

    def learning_runner(self) -> CloudLearningRunner:
        return ReadOnlyEvidenceWorkflowRunner(
            SmartEssReadOnlyEvidenceOperation()
        )


class UnavailableCloudLearningEngine(CloudLearningEngine):
    """Fail-closed result for malformed or unregistered selections."""

    def __init__(self, requested_source_id: object = "") -> None:
        requested = (
            requested_source_id
            if type(requested_source_id) is str
            and requested_source_id
            and requested_source_id == requested_source_id.strip()
            else "unknown"
        )
        self._requested = requested
        self.adapter = UnavailableCloudApiAdapter(requested)
        self.method = None
        self.evidence_capabilities = NO_LOCAL_EVIDENCE
        self.default_for_method = False

    @property
    def available(self) -> bool:
        return False

    def learning_runner(self) -> CloudLearningRunner:
        return UnavailableCloudLearningRunner(
            "" if self._requested == "unknown" else self._requested
        )


_REGISTERED_ENGINES: tuple[CloudLearningEngine, ...] = (
    DessMonitorActiveCloudLearningEngine(),
    DessMonitorCloudLearningEngine(),
    SmartEssCloudLearningEngine(),
    SmartEssReadOnlyCloudLearningEngine(),
    ValueCloudCloudLearningEngine(),
)


def _engine_key(selection: CloudLearningSelection) -> tuple[str, str]:
    return selection.method_id, selection.source_id


def _validate_registered_engines(
    engines: tuple[CloudLearningEngine, ...],
) -> None:
    """Reject malformed, incompatible or ambiguous registry declarations."""

    selection_keys: set[tuple[str, str]] = set()
    for engine in engines:
        if not isinstance(engine, CloudLearningEngine):
            raise TypeError("cloud_learning_registered_engine_invalid")
        if type(engine.source) is not CloudApiSource:
            raise TypeError("cloud_learning_registered_source_invalid")
        if type(engine.method) is not CloudLearningMethod:
            raise TypeError("cloud_learning_registered_method_invalid")
        if type(engine.evidence_capabilities) is not CloudLearningEvidenceCapabilities:
            raise TypeError("cloud_learning_registered_evidence_invalid")
        if type(engine.default_for_method) is not bool:
            raise TypeError("cloud_learning_registered_default_invalid")
        if not source_supports_method(engine.source, engine.method):
            raise ValueError("cloud_learning_registered_selection_incompatible")
        selection = engine.selection
        if type(selection) is not CloudLearningSelection:
            raise TypeError("cloud_learning_registered_selection_invalid")
        key = _engine_key(selection)
        if key in selection_keys:
            raise ValueError("cloud_learning_registered_selection_duplicate")
        selection_keys.add(key)


_validate_registered_engines(_REGISTERED_ENGINES)

_ENGINES_BY_SELECTION: dict[tuple[str, str], CloudLearningEngine] = {}
for _registered_engine in _REGISTERED_ENGINES:
    _registered_selection = _registered_engine.selection
    if type(_registered_selection) is not CloudLearningSelection:
        raise TypeError("cloud_learning_registered_selection_invalid")
    _ENGINES_BY_SELECTION[_engine_key(_registered_selection)] = _registered_engine

def supported_cloud_learning_sources() -> tuple[CloudApiSource, ...]:
    """Return every registered API source in stable source-id order."""

    sources = {
        engine.source.source_id: engine.source for engine in _REGISTERED_ENGINES
    }
    return tuple(sources[key] for key in sorted(sources))


def supported_cloud_learning_methods() -> tuple[CloudLearningMethod, ...]:
    """Return every registered product method in stable method-id order."""

    methods = {
        engine.method.method_id: engine.method
        for engine in _REGISTERED_ENGINES
        if type(engine.method) is CloudLearningMethod
    }
    return tuple(methods[key] for key in sorted(methods))


def supported_cloud_learning_selections() -> tuple[CloudLearningSelection, ...]:
    """Return every exact executable method/source binding."""

    selections = tuple(
        engine.selection
        for engine in _REGISTERED_ENGINES
        if type(engine.selection) is CloudLearningSelection
    )
    return tuple(sorted(selections, key=_engine_key))


def compatible_cloud_learning_sources(provider_id: object) -> tuple[CloudApiSource, ...]:
    """Return sources owned by one exact normalized evidence provider."""

    if type(provider_id) is not str or provider_id != provider_id.strip():
        return ()
    return tuple(
        source
        for source in supported_cloud_learning_sources()
        if source.provider_id == provider_id
    )


def compatible_cloud_learning_sources_for_method(
    provider_id: object,
    method_id: object,
) -> tuple[CloudApiSource, ...]:
    """Return exact API sources registered for one provider and method."""

    if (
        type(provider_id) is not str
        or provider_id != provider_id.strip()
        or type(method_id) is not str
        or method_id != method_id.strip()
    ):
        return ()
    return tuple(
        engine.source
        for engine in _REGISTERED_ENGINES
        if engine.source.provider_id == provider_id
        and engine.method.method_id == method_id
        and source_supports_method(engine.source, engine.method)
    )


def compatible_cloud_learning_sources_for_method_any_provider(
    method_id: object,
) -> tuple[CloudApiSource, ...]:
    """Return exact API sources for a method without guessing a provider.

    This boundary is used only by metadata-only support acquisition when the
    collector's cloud family is not known yet.  The user selects the API
    explicitly; active route-owning methods continue to require a trusted
    provider before this function is ever consulted by the options flow.
    """

    if type(method_id) is not str or method_id != method_id.strip():
        return ()
    return tuple(
        engine.source
        for engine in _REGISTERED_ENGINES
        if engine.method.method_id == method_id
        and source_supports_method(engine.source, engine.method)
    )


def compatible_cloud_learning_methods(
    source_id: object,
) -> tuple[CloudLearningMethod, ...]:
    """Return exact product methods registered for one API source."""

    if type(source_id) is not str or source_id != source_id.strip():
        return ()
    return tuple(
        engine.method
        for engine in _REGISTERED_ENGINES
        if engine.source.source_id == source_id
        and source_supports_method(engine.source, engine.method)
    )


def compatible_cloud_learning_methods_for_provider(
    provider_id: object,
) -> tuple[CloudLearningMethod, ...]:
    """Return product methods executable for one trusted provider."""

    if type(provider_id) is not str or provider_id != provider_id.strip():
        return ()
    methods = {
        engine.method.method_id: engine.method
        for engine in _REGISTERED_ENGINES
        if engine.source.provider_id == provider_id
        and type(engine.method) is CloudLearningMethod
    }
    preferred_order = (
        READ_ONLY_EVIDENCE_METHOD.method_id,
        ACTIVE_CORRELATION_METHOD.method_id,
    )
    return tuple(methods[key] for key in preferred_order if key in methods)


def default_cloud_learning_method(provider_id: object) -> str:
    """Prefer read-only analysis, otherwise return the sole available method."""

    compatible = compatible_cloud_learning_methods_for_provider(provider_id)
    method_ids = tuple(method.method_id for method in compatible)
    if READ_ONLY_EVIDENCE_METHOD.method_id in method_ids:
        return READ_ONLY_EVIDENCE_METHOD.method_id
    return method_ids[0] if len(method_ids) == 1 else ""


def default_cloud_learning_source_for_method(
    provider_id: object,
    method_id: object,
) -> str:
    """Return the sole binding default for one exact provider/method pair."""

    if type(method_id) is not str or method_id != method_id.strip():
        return ""
    defaults = tuple(
        engine.source.source_id
        for engine in _REGISTERED_ENGINES
        if engine.source.provider_id == provider_id
        and engine.method is not None
        and engine.method.method_id == method_id
        and engine.default_for_method
    )
    return defaults[0] if len(defaults) == 1 else ""


def default_cloud_learning_source_for_method_any_provider(
    method_id: object,
) -> str:
    """Return one registry-declared default without provider inference."""

    if type(method_id) is not str or method_id != method_id.strip():
        return ""
    defaults = tuple(
        engine.source.source_id
        for engine in _REGISTERED_ENGINES
        if engine.method.method_id == method_id and engine.default_for_method
    )
    return defaults[0] if len(defaults) == 1 else ""


def resolve_cloud_learning_selection(selection: object) -> CloudLearningEngine:
    """Resolve one strict method/source pair; malformed values fail closed."""

    if type(selection) is not CloudLearningSelection:
        return UnavailableCloudLearningEngine()
    return _ENGINES_BY_SELECTION.get(
        _engine_key(selection),
        UnavailableCloudLearningEngine(selection.source_id),
    )


__all__ = [
    "CloudLearningEngine",
    "DessMonitorActiveCloudLearningEngine",
    "DessMonitorCloudLearningEngine",
    "SmartEssCloudLearningEngine",
    "SmartEssReadOnlyCloudLearningEngine",
    "UnavailableCloudLearningEngine",
    "ValueCloudCloudLearningEngine",
    "compatible_cloud_learning_methods",
    "compatible_cloud_learning_methods_for_provider",
    "compatible_cloud_learning_sources",
    "compatible_cloud_learning_sources_for_method",
    "compatible_cloud_learning_sources_for_method_any_provider",
    "default_cloud_learning_method",
    "default_cloud_learning_source_for_method",
    "default_cloud_learning_source_for_method_any_provider",
    "resolve_cloud_learning_selection",
    "supported_cloud_learning_methods",
    "supported_cloud_learning_selections",
    "supported_cloud_learning_sources",
]
