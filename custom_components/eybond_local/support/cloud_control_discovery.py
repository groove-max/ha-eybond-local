"""Provider-owned cloud control-discovery runners.

Each provider owns its cloud login/session, device-bundle fetch, signed/control
action construction, provider response parsing, control-action execution (via the
provider orchestrator), and conversion to a normalized discovery outcome. The
config flow owns only HA orchestration (progress, state, artifacts, user
confirmations) and passes callbacks in -- it imports no provider HTTP client and
parses no provider payload.

Request order, progress fractions, error codes, and the safety semantics of the
orchestrators are preserved exactly from the previous in-flow implementation.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .. import valuecloud_cloud as valuecloud_cloud_module
from ..smartess_cloud import (
    build_device_detail_action,
    build_device_settings_action,
    fetch_device_bundle_for_collector,
    fetch_signed_action,
    login_with_password,
)
from .read_learning_binder import bind_cloud_labels_to_registers
from .shadow_learning.orchestrator import async_orchestrate_shadow_learning_settings
from .shadow_learning.valuecloud_orchestrator import (
    async_orchestrate_valuecloud_shadow_learning,
)

logger = logging.getLogger(__name__)

# Executor is HA's ``hass.async_add_executor_job`` (blocking client work runs
# there). ``progress`` is the flow's determinate-progress hook. ``on_identity`` /
# ``on_learning`` let the flow update its own UI state at the same points as
# before -- the runner never touches flow state directly.
ExecutorJob = Callable[..., Awaitable[Any]]
ProgressCallback = Callable[..., None]
IdentityCallback = Callable[[dict[str, Any]], None]
LearningCallback = Callable[[], None]
StartShadowRouteCallback = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class ControlDiscoveryOutcome:
    """Normalized result of one provider cloud control-discovery run."""

    identity: dict[str, Any]
    result: dict[str, Any]
    read_bindings: dict[str, Any] | None = None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _identity_from_bundle(bundle: Any) -> dict[str, Any] | None:
    """Return pn/sn/devcode/devaddr from a live provider device bundle."""

    if not isinstance(bundle, dict):
        return None
    request = bundle.get("request")
    if not isinstance(request, dict):
        return None
    params = request.get("params")
    if not isinstance(params, dict):
        return None
    pn = str(params.get("pn") or "").strip()
    sn = str(params.get("sn") or "").strip()
    devcode = _coerce_int(params.get("devcode"))
    devaddr = _coerce_int(params.get("devaddr"))
    if not pn or not sn or devcode is None or devaddr is None:
        return None
    return {"pn": pn, "sn": sn, "devcode": devcode, "devaddr": devaddr}


def _settings_dat_from_bundle(bundle: Any) -> dict[str, Any] | None:
    """Return raw SmartESS settings dat from a live device bundle."""

    if not isinstance(bundle, dict):
        return None
    responses = bundle.get("responses")
    if not isinstance(responses, dict):
        return None
    settings = responses.get("device_settings")
    if not isinstance(settings, dict):
        return None
    dat = settings.get("dat")
    return dict(dat) if isinstance(dat, dict) else None


class CloudControlDiscoveryRunner(ABC):
    """One provider's cloud control-discovery runner."""

    provider_id: str = ""

    @abstractmethod
    async def async_run(
        self,
        *,
        executor: ExecutorJob,
        collector_pn: str,
        username: str,
        password: str,
        fallback_identity: dict[str, Any] | None,
        max_fields: int,
        progress: ProgressCallback,
        orchestrator_callbacks: Mapping[str, Any],
        on_identity: IdentityCallback,
        start_shadow_route: StartShadowRouteCallback,
        on_learning: LearningCallback,
    ) -> ControlDiscoveryOutcome:
        ...


class SmartEssControlDiscoveryRunner(CloudControlDiscoveryRunner):
    provider_id = "smartess"

    async def async_run(
        self,
        *,
        executor,
        collector_pn,
        username,
        password,
        fallback_identity,
        max_fields,
        progress,
        orchestrator_callbacks,
        on_identity,
        start_shadow_route,
        on_learning,
    ) -> ControlDiscoveryOutcome:
        progress(0.08, "fetching")
        await start_shadow_route()
        progress(0.18, "fetching")
        cloud_bundle = await executor(
            lambda: fetch_device_bundle_for_collector(
                username=username,
                password=password,
                collector_pn=str(collector_pn or ""),
            )
        )
        identity = _identity_from_bundle(cloud_bundle) or fallback_identity
        if identity is None:
            raise RuntimeError("shadow_learning_identity_unavailable")
        on_identity(identity)

        # SmartESS serializes collector-bound work within one authenticated
        # session. The full provider bundle owns its metadata login; a fresh
        # post-bundle login is the control-dispatch boundary. This exact route ->
        # bundle -> control-login -> control sequence is pinned by the successful
        # production trace and must not be collapsed into one session.
        _control_login, control_session = await executor(
            lambda: login_with_password(username=username, password=password)
        )

        settings_dat = _settings_dat_from_bundle(cloud_bundle)
        if settings_dat is None:
            settings_envelope = await executor(
                lambda: fetch_signed_action(
                    action=build_device_settings_action(
                        pn=identity["pn"],
                        sn=identity["sn"],
                        devcode=identity["devcode"],
                        devaddr=identity["devaddr"],
                    ),
                    session=control_session,
                )
            )
            settings_dat = settings_envelope.dat

        on_learning()
        progress(0.30, "testing")
        result = await async_orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=control_session,
            pn=identity["pn"],
            sn=identity["sn"],
            devcode=identity["devcode"],
            devaddr=identity["devaddr"],
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=True,
            all_choice_values=True,
            max_fields=max_fields,
            continue_on_error=True,
            delay_seconds=0.0,
            **dict(orchestrator_callbacks),
        )
        if int(result.get("degraded_count") or 0) > 0:
            first_result = next(
                (
                    item
                    for item in result.get("results", ())
                    if isinstance(item, dict)
                ),
                {},
            )
            response = first_result.get("response")
            logger.warning(
                "SmartESS control discovery degraded planned=%s executed=%s "
                "first_status=%s first_reason=%s first_response_err=%s",
                int(result.get("planned_write_count") or 0),
                int(result.get("executed_result_count") or 0),
                str(first_result.get("status") or ""),
                str(first_result.get("reason") or ""),
                (
                    response.get("err")
                    if isinstance(response, dict)
                    else ""
                ),
            )

        read_bindings: dict[str, Any] | None = None
        read_map = result.get("read_map")
        if isinstance(read_map, dict) and read_map.get("registers"):
            read_bindings = await self._async_bind_read_labels(
                executor=executor,
                cloud_session=control_session,
                identity=identity,
                read_map=read_map,
            )
        return ControlDiscoveryOutcome(
            identity=identity, result=result, read_bindings=read_bindings
        )

    async def _async_bind_read_labels(
        self,
        *,
        executor,
        cloud_session,
        identity: dict[str, Any],
        read_map: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Correlate the cloud's labeled sensors against the session read map.

        Best-effort: nothing here may fail the discovery run after the probe
        sweep already succeeded.
        """

        registers = read_map.get("registers")
        if not isinstance(registers, dict) or not registers:
            return None
        try:
            envelope = await executor(
                lambda: fetch_signed_action(
                    action=build_device_detail_action(
                        pn=str(identity.get("pn") or ""),
                        sn=str(identity.get("sn") or ""),
                        devcode=int(identity.get("devcode") or 0),
                        devaddr=int(identity.get("devaddr") or 1),
                    ),
                    session=cloud_session,
                )
            )
            dat = envelope.dat if isinstance(envelope.dat, dict) else {}
            pars = dat.get("pars") if isinstance(dat.get("pars"), dict) else {}
            sensors: list[dict[str, Any]] = []
            for items in pars.values():
                if isinstance(items, list):
                    sensors.extend(item for item in items if isinstance(item, dict))
            if not sensors:
                return None
            report = bind_cloud_labels_to_registers(sensors=sensors, registers=registers)
            return report.to_json_dict()
        except Exception as exc:  # noqa: BLE001 - best-effort supplemental step
            logger.debug("Read-label binding failed during learning session: %s", exc)
            return None


class ValueCloudControlDiscoveryRunner(CloudControlDiscoveryRunner):
    provider_id = "valuecloud"

    async def async_run(
        self,
        *,
        executor,
        collector_pn,
        username,
        password,
        fallback_identity,
        max_fields,
        progress,
        orchestrator_callbacks,
        on_identity,
        start_shadow_route,
        on_learning,
    ) -> ControlDiscoveryOutcome:
        progress(0.08, "fetching")
        _login_envelope, cloud_session = await executor(
            lambda: valuecloud_cloud_module.login_with_password(
                username=username, password=password
            )
        )
        await start_shadow_route()
        progress(0.18, "fetching")
        cloud_bundle = await executor(
            lambda: valuecloud_cloud_module.fetch_device_bundle_for_collector_with_session(
                session=cloud_session,
                collector_pn=str(collector_pn or ""),
            )
        )
        identity = _identity_from_bundle(cloud_bundle) or fallback_identity
        if identity is None:
            raise RuntimeError("shadow_learning_identity_unavailable")
        on_identity(identity)

        normalized = cloud_bundle.get("normalized") if isinstance(cloud_bundle, dict) else None
        batch_control = (
            normalized.get("batch_control")
            if isinstance(normalized, dict) and isinstance(normalized.get("batch_control"), dict)
            else None
        )
        control_strategy = (
            normalized.get("control_strategy")
            if isinstance(normalized, dict) and isinstance(normalized.get("control_strategy"), dict)
            else None
        )
        device_ctrl = (
            normalized.get("device_ctrl")
            if isinstance(normalized, dict) and isinstance(normalized.get("device_ctrl"), dict)
            else None
        )
        if (
            not isinstance(batch_control, dict)
            and not isinstance(control_strategy, dict)
            and not isinstance(device_ctrl, dict)
        ):
            raise RuntimeError("valuecloud_batch_control_unavailable")

        on_learning()
        progress(0.30, "testing")
        result = await async_orchestrate_valuecloud_shadow_learning(
            batch_control=batch_control,
            control_strategy=control_strategy,
            device_ctrl=device_ctrl,
            session=cloud_session,
            pn=identity["pn"],
            sn=identity["sn"],
            devcode=identity["devcode"],
            devaddr=identity["devaddr"],
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=True,
            all_choice_values=True,
            max_fields=max_fields,
            continue_on_error=True,
            delay_seconds=0.0,
            **dict(orchestrator_callbacks),
        )
        return ControlDiscoveryOutcome(identity=identity, result=result, read_bindings=None)


class UnavailableControlDiscoveryRunner(CloudControlDiscoveryRunner):
    provider_id = ""

    def __init__(self, requested_provider_id: str = "") -> None:
        self._requested = str(requested_provider_id or "").strip().lower()

    async def async_run(self, **_kwargs) -> ControlDiscoveryOutcome:
        raise RuntimeError(
            f"control_discovery_provider_not_supported:{self._requested or 'unknown'}"
        )
