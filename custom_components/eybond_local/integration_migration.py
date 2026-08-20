"""Config-entry schema migration authority."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

logger = logging.getLogger(__name__)

_ENTRY_SCHEMA_VERSION = 5


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Migrate a config entry to the explicit connection architecture axes.

    Version 2 added ``connection_strategy`` / ``endpoint_control_policy`` /
    ``proxy_enabled`` as explicit, opaque, hostname-free entry state, derived
    deterministically from the legacy operation-mode / connection-mode /
    endpoint-provenance fields (see :mod:`connection.connection_policy`).

    Version 3 is a corrective re-migration: an earlier v2 pass could derive
    ``connection_strategy=inbound`` for a cloud-primary factory collector because
    a stale ``callback_listener`` connection_mode used to take precedence over the
    operation mode. Such an entry can never connect as inbound (the collector
    points at the vendor cloud and never dials Home Assistant), so it is corrected
    to ``callback_on_demand``. The correction is narrow: only the cloud-primary
    cloud+HA shape is touched (never manual/known-IP), so a genuinely-explicit
    inbound value is never overwritten just because the legacy connection_mode
    looks user-triggered. No endpoint is ever written during migration.

    Version 4 makes ``entry.data`` the SINGLE canonical owner of
    ``connection_strategy``. Before v4 the options form wrote the strategy into
    ``entry.options`` while the explicit endpoint actions (HA-only / Cloud+HA
    switch, bind, rollback) wrote it into ``entry.data``, and the resolver read
    options first -- so a stale options copy silently shadowed the result of a
    successful action. v4 freezes the strategy the entry ACTUALLY behaved with
    (computed with the OLD options-first semantics, so a conflicting data/options
    pair keeps its real pre-upgrade behavior rather than being "healed" by a
    guess) into ``entry.data`` and DELETES the options copy. The value is never
    re-derived from hostname, endpoint, cloud provider, collector kind or peer IP.

    Version 5 introduces the typed RecoveryContract era
    (:mod:`connection.recovery_contract`) and deliberately writes NOTHING:

    * ``recovery_contract`` (the ONE canonical key, in ``entry.data``) is only
      ever created by a REAL recovery verifier, none of which exist yet;
    * the only legacy evidence that could conservatively map to a proof --
      ``connection_strategy_evidence=reboot_reconnect`` -- has NO persisted
      verification timestamp and NO strong identity source in any <=v4 schema,
      and inventing the migration time as ``verified_at`` is forbidden, so no
      contract is created (the model deliberately has no legacy proof method
      to backfill into; the rule is pinned by the migration tests);
    * ``callback_trigger`` evidence is identity bookkeeping, never recovery;
      ``user_confirmed_session`` is a user binding, never a reboot proof;
    * legacy evidence fields remain untouched for the compatibility reader;
      connection_strategy / endpoint_control_policy / endpoints / collector IP
      are not modified, and no network I/O happens.

    The v4->v5 step is therefore a pure version bump.

    Only missing axis fields are filled and only the provably mis-migrated
    strategy is corrected; all other legacy fields are left untouched for
    backward compatibility.
    """

    from .connection.connection_policy import (
        correct_migrated_connection_strategy,
        legacy_effective_connection_strategy,
        legacy_options_strategy_keys,
        migrate_entry_axes,
    )
    from .const import CONF_CONNECTION_STRATEGY

    version = int(getattr(entry, "version", 1) or 1)
    if version > _ENTRY_SCHEMA_VERSION:
        # A newer schema than this code understands: refuse rather than corrupt.
        return False
    if version >= _ENTRY_SCHEMA_VERSION:
        return True

    data = dict(entry.data)
    options = dict(entry.options)
    changed = False
    options_changed = False

    if version < 4:
        # v3 -> v4 (FIRST): freeze the entry's real pre-upgrade effective
        # strategy -- computed with the OLD options-first rule -- into data,
        # before any later step reads it back. A conflicting data/options pair
        # is preserved exactly as it behaved; no heuristic "healing".
        pre_upgrade_strategy = legacy_effective_connection_strategy(data, options)
        if data.get(CONF_CONNECTION_STRATEGY) != pre_upgrade_strategy:
            data[CONF_CONNECTION_STRATEGY] = pre_upgrade_strategy
            changed = True
        # ... and drop the options copy so it can never shadow data again.
        for key in legacy_options_strategy_keys():
            if key in options:
                del options[key]
                options_changed = True

        # v1 -> v2: fill any missing axes (idempotent; explicit axes are
        # preserved).
        for key, value in migrate_entry_axes(data, options).items():
            if data.get(key) != value:
                data[key] = value
                changed = True

        # v2 -> v3: correct a provably-broken cloud-primary inbound entry.
        # Safe, deterministic, and only in the inbound -> callback_on_demand
        # direction.
        corrected_strategy = correct_migrated_connection_strategy(data, options)
        if corrected_strategy is not None and data.get(CONF_CONNECTION_STRATEGY) != corrected_strategy:
            logger.warning(
                "EyeBond entry %s: correcting unreachable inbound cloud-primary entry to %s",
                entry.entry_id,
                corrected_strategy,
            )
            data[CONF_CONNECTION_STRATEGY] = corrected_strategy
            changed = True

    # v4 -> v5: the RecoveryContract era. A pure version bump -- see the
    # docstring for why NO legacy evidence may be converted into a proof here.

    update_kwargs: dict[str, Any] = {"version": _ENTRY_SCHEMA_VERSION}
    if changed:
        update_kwargs["data"] = data
    if options_changed:
        update_kwargs["options"] = options
    try:
        hass.config_entries.async_update_entry(entry, **update_kwargs)
    except TypeError:
        # Older cores do not accept ``version=`` on async_update_entry.
        legacy_kwargs: dict[str, Any] = {}
        if changed:
            legacy_kwargs["data"] = data
        if options_changed:
            legacy_kwargs["options"] = options
        if legacy_kwargs:
            hass.config_entries.async_update_entry(entry, **legacy_kwargs)
        try:
            entry.version = _ENTRY_SCHEMA_VERSION  # type: ignore[misc]
        except Exception:
            pass
    logger.info(
        "Migrated EyeBond entry %s to schema v%s (axes %s)",
        entry.entry_id,
        _ENTRY_SCHEMA_VERSION,
        {
            CONF_CONNECTION_STRATEGY: data.get(CONF_CONNECTION_STRATEGY),
        },
    )
    return True
