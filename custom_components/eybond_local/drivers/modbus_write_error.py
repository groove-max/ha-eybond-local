"""Shared Modbus write-error classification (driver-owned protocol policy).

This is the single authority that understands ``ModbusError`` and Modbus
exception codes 1/2/3/7. It lives in the driver layer -- not in the runtime hub
and not in the payload transport -- and writable Modbus drivers opt in by mixing
in :class:`ModbusWriteErrorMixin`.

Semantics are preserved exactly from the previous in-hub implementation:

* code 1 -> ``illegal_function`` blocker (clear on redetect);
* code 2 -> ``illegal_data_address`` blocker (clear on redetect);
* code 3 -> non-persistent user-facing ``illegal_data_value`` error;
* code 7 -> ``mode_restricted`` (unsafe while running in an unsafe mode) or
  ``unsupported_or_locked`` blocker;
* any other / non-Modbus error -> empty classification (hub re-raises original).
"""

from __future__ import annotations

from ..models import CapabilityBlocker, WriteCapability
from ..payload.modbus import ModbusError
from .write_error import EMPTY_WRITE_ERROR_CLASSIFICATION, WriteErrorClassification


def _modbus_exception_code(exc: BaseException) -> int | None:
    """Parse one Modbus exception code from an error string."""

    if not isinstance(exc, ModbusError):
        return None

    text = str(exc)
    if not text.startswith("exception_code:"):
        return None
    try:
        return int(text.split(":", 1)[1])
    except ValueError:
        return None


def _blocker_from_exception_code(
    capability: WriteCapability,
    exception_code: int,
    *,
    operating_mode: object,
) -> CapabilityBlocker | None:
    """Return one structured runtime blocker for a Modbus write rejection."""

    capability_name = capability.display_name
    safe_modes = ", ".join(capability.safe_operating_modes)

    if exception_code == 1:
        return CapabilityBlocker(
            code="illegal_function",
            reason=(
                f"The inverter does not expose writable access for {capability_name!r} "
                "through this protocol."
            ),
            suggested_action=(
                "Leave this control disabled for the current firmware, or retry after "
                "updating the driver/profile."
            ),
            exception_code=exception_code,
            clear_on="redetect",
        )
    if exception_code == 2:
        return CapabilityBlocker(
            code="illegal_data_address",
            reason=(
                f"The inverter reported register {capability.register} for "
                f"{capability_name!r} as unavailable."
            ),
            suggested_action=(
                "This register is likely absent on the current model or firmware. "
                "Leave it disabled unless a later probe confirms support."
            ),
            exception_code=exception_code,
            clear_on="redetect",
        )
    if exception_code == 7:
        if (
            capability.unsafe_while_running
            and operating_mode
            and operating_mode not in capability.safe_operating_modes
        ):
            return CapabilityBlocker(
                code="mode_restricted",
                reason=(
                    f"The inverter rejected writes to {capability_name!r} while "
                    f"operating mode is {operating_mode!r}."
                ),
                suggested_action=(
                    "Retry after switching the inverter into a safe mode for this setting: "
                    f"{safe_modes}."
                ),
                exception_code=exception_code,
                clear_on="mode_change",
            )
        return CapabilityBlocker(
            code="unsupported_or_locked",
            reason=(
                f"The inverter rejected writes to {capability_name!r}. "
                "This register appears locked or unsupported by the current firmware."
            ),
            suggested_action=(
                "Keep this control disabled for now, or retry after a firmware/profile update."
            ),
            exception_code=exception_code,
            clear_on="redetect",
        )
    return None


def _user_error_from_exception_code(
    capability: WriteCapability,
    exception_code: int,
) -> ValueError | None:
    """Return one user-facing write error that should not persist as a blocker."""

    if exception_code != 3:
        return None

    native_minimum = capability.native_minimum
    native_maximum = capability.native_maximum
    if native_minimum is not None and native_maximum is not None:
        allowed_range = f"Allowed profile range: {native_minimum} to {native_maximum}."
    elif native_minimum is not None:
        allowed_range = f"Allowed profile minimum: {native_minimum}."
    elif native_maximum is not None:
        allowed_range = f"Allowed profile maximum: {native_maximum}."
    else:
        allowed_range = "The inverter may enforce a narrower range than the current profile metadata."

    return ValueError(
        f"illegal_data_value:{capability.key}:"
        f"The inverter rejected {capability.display_name!r} as out of range. "
        f"{allowed_range}"
    )


def classify_modbus_write_error(
    capability: WriteCapability,
    exc: BaseException,
    *,
    operating_mode: object,
) -> WriteErrorClassification:
    """Classify a failed Modbus capability write into a neutral verdict."""

    exception_code = _modbus_exception_code(exc)
    if exception_code is None:
        return EMPTY_WRITE_ERROR_CLASSIFICATION

    user_error = _user_error_from_exception_code(capability, exception_code)
    if user_error is not None:
        return WriteErrorClassification(user_error=user_error)

    blocker = _blocker_from_exception_code(
        capability,
        exception_code,
        operating_mode=operating_mode,
    )
    if blocker is not None:
        return WriteErrorClassification(blocker=blocker)
    return EMPTY_WRITE_ERROR_CLASSIFICATION


class ModbusWriteErrorMixin:
    """Opt-in Modbus write-error classification for writable Modbus drivers."""

    def classify_write_error(
        self,
        capability: WriteCapability,
        exc: BaseException,
        *,
        operating_mode: object = None,
    ) -> WriteErrorClassification:
        """Classify a failed capability write using shared Modbus policy."""

        return classify_modbus_write_error(
            capability,
            exc,
            operating_mode=operating_mode,
        )
