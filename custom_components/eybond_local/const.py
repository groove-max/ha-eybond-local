"""Constants for the EyeBond Local integration."""

DOMAIN = "eybond_local"
PLATFORMS: list[str] = ["sensor", "binary_sensor", "number", "select", "switch", "button", "text"]
LOCAL_METADATA_DIR = "eybond_local"
LOCAL_PROFILES_DIR = "profiles"
LOCAL_REGISTER_SCHEMAS_DIR = "register_schemas"
LOCAL_CLOUD_EVIDENCE_DIR = "cloud_evidence"
LOCAL_PROXY_TRACES_DIR = "proxy_traces"
LOCAL_SUPPORT_PACKAGES_DIR = "support_packages"
LOCAL_DIAGNOSTIC_RUNS_DIR = "diagnostic_runs"
BUILTIN_SCHEMA_PREFIX = "builtin:"

CONF_SERVER_IP = "server_ip"
CONF_ADVERTISED_SERVER_IP = "advertised_server_ip"
CONF_COLLECTOR_IP = "collector_ip"
CONF_COLLECTOR_PN = "collector_pn"
CONF_COLLECTOR_CLOUD_FAMILY = "collector_cloud_family"
# Durable, provider-neutral CONFIRMED collector wire evidence. Written ONLY from
# a trusted, non-conflicting live SessionHandle whose durable PN matches the
# entry. It may seed a same-PN reconnect/startup bootstrap; a live SessionHandle
# always overrides it. Cloud family / endpoint / collector kind / driver key /
# peer IP can never create it. The legacy ``collector_session_protocol`` field is
# an INFERRED cloud-family hint (diagnostic only) and must never be treated as
# confirmed -- migration is fail-closed (no provenance invented for old data).
CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL = "collector_confirmed_session_protocol"
CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE = "collector_confirmed_session_protocol_source"
CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN = "collector_confirmed_session_protocol_pn"
CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT = "collector_confirmed_session_protocol_observed_at"
# The only accepted provenance source for confirmed wire evidence.
COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE = "live_session"
CONF_COLLECTOR_OPERATION_MODE = "collector_operation_mode"
CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT = "collector_original_server_endpoint"
CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT = "collector_original_server_endpoint_observed_at"
CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY = "collector_original_server_endpoint_profile_key"
CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE = "collector_original_server_endpoint_source"
CONF_CONNECTION_TYPE = "connection_type"
CONF_CONNECTION_MODE = "connection_mode"
CONF_CONTROL_MODE = "control_mode"
CONF_DETECTION_CONFIDENCE = "detection_confidence"
CONF_DETECTED_MODEL = "detected_model"
CONF_DETECTED_SERIAL = "detected_serial"
CONF_DEVICE_CATALOG_KIND = "device_catalog_kind"
CONF_DEVICE_CATALOG_TIER = "device_catalog_tier"
CONF_DEVICE_CATALOG_ENTRY = "device_catalog_entry_key"
CONF_SMARTESS_COLLECTOR_VERSION = "smartess_collector_version"
CONF_SMARTESS_PROTOCOL_ASSET_ID = "smartess_protocol_asset_id"
CONF_SMARTESS_PROFILE_KEY = "smartess_profile_key"
CONF_SMARTESS_DEVICE_ADDRESS = "smartess_device_address"
CONF_TCP_PORT = "tcp_port"
CONF_ADVERTISED_TCP_PORT = "advertised_tcp_port"
CONF_UDP_PORT = "udp_port"
CONF_DISCOVERY_TARGET = "discovery_target"
CONF_DISCOVERY_INTERVAL = "discovery_interval"
CONF_HEARTBEAT_INTERVAL = "heartbeat_interval"
CONF_POLL_INTERVAL = "poll_interval"
CONF_POLL_MODE = "poll_mode"
CONF_DRIVER_HINT = "driver_hint"
CONF_PROXY_CAPTURE_DURATION_MINUTES = "proxy_capture_duration_minutes"
CONF_ENTRY_ROLE = "entry_role"

# Integration-level config entry that keeps the passive callback listeners
# loaded even when no collector/device entries exist. It owns no device,
# coordinator, endpoint, or collector session.
ENTRY_ROLE_LISTENER = "listener"

# A collector the user saved BEFORE its durable full PN was known. It is not a
# normal collector entry: it starts no coordinator/runtime, creates no devices or
# entities, never writes an endpoint, and never claims a session by address. It
# only carries the user's canonical connection_strategy choice plus (for
# callback_on_demand) the target address to trigger. It is promoted to a normal
# collector entry -- in place, same entry -- once a durable full PN is proven.
ENTRY_ROLE_PENDING_COLLECTOR = "pending_collector"

# Pending-only entry.data fields. All of these are deleted at promotion; none of
# them is ever an identity.
#
# The synthetic pending identity (`pending:<ULID>`) mirrored from unique_id, so a
# pending entry has a stable id that is NOT derived from an address. Two pending
# entries behind one NAT/peer IP are therefore distinct.
CONF_PENDING_ID = "pending_id"
# The address a callback_on_demand pending entry triggers. For an inbound pending
# entry this is a user-entered HINT only (diagnostics/UI), never identity and
# never a reason to bind a session.
CONF_PENDING_ADDRESS_HINT = "pending_address_hint"
# Typed outcome of the last pending attempt, surfaced in the options flow. Never
# raw exception text.
CONF_PENDING_LAST_ATTEMPT_RESULT = "pending_last_attempt_result"

PENDING_UNIQUE_ID_PREFIX = "pending:"

# Typed pending attempt outcomes (translation keys; never raw exception text).
PENDING_ATTEMPT_WAITING_INBOUND = "waiting_inbound"
PENDING_ATTEMPT_WAITING_CALLBACK = "waiting_callback"
PENDING_ATTEMPT_CALLBACK_TIMEOUT = "callback_timeout"
PENDING_ATTEMPT_TARGET_UNAVAILABLE = "callback_target_unavailable"
PENDING_ATTEMPT_IDENTITY_NOT_CONFIRMED = "identity_not_confirmed"
# More than one DISTINCT strong identity appeared after this attempt's trigger.
# The attempt cannot prove which collector answered IT, so nothing is bound.
PENDING_ATTEMPT_IDENTITY_AMBIGUOUS = "callback_identity_ambiguous"
# Another callback trigger was recorded (any entry/flow) while this attempt was
# in flight, so a new session is not attributable to THIS trigger.
PENDING_ATTEMPT_TRIGGER_INTERFERENCE = "callback_trigger_interference"
# OUR OWN failure to get the trigger sequence out (an inhibited window, a socket
# error, a competing attempt holding the causality lease). Deliberately NOT
# folded into interference: that told the user to hunt a phantom competing flow.
PENDING_ATTEMPT_TRIGGER_NOT_SENT = "callback_trigger_not_sent"
# The session opened but its identity could not be read authoritatively.
PENDING_ATTEMPT_IDENTITY_UNVERIFIED = "callback_identity_unverified"
# The identity is owned by another entry/flow.
PENDING_ATTEMPT_IDENTITY_CLAIMED_BY_OTHER = "callback_identity_conflict"
PENDING_ATTEMPT_CANDIDATE_READY = "candidate_ready"
PENDING_ATTEMPT_PROMOTED = "promoted"
PENDING_ATTEMPT_RESULTS = {
    PENDING_ATTEMPT_WAITING_INBOUND,
    PENDING_ATTEMPT_WAITING_CALLBACK,
    PENDING_ATTEMPT_CALLBACK_TIMEOUT,
    PENDING_ATTEMPT_TARGET_UNAVAILABLE,
    PENDING_ATTEMPT_IDENTITY_NOT_CONFIRMED,
    PENDING_ATTEMPT_IDENTITY_AMBIGUOUS,
    PENDING_ATTEMPT_TRIGGER_INTERFERENCE,
    PENDING_ATTEMPT_TRIGGER_NOT_SENT,
    PENDING_ATTEMPT_IDENTITY_UNVERIFIED,
    PENDING_ATTEMPT_IDENTITY_CLAIMED_BY_OTHER,
    PENDING_ATTEMPT_CANDIDATE_READY,
    PENDING_ATTEMPT_PROMOTED,
}

DEFAULT_TCP_PORT = 8899
DEFAULT_UDP_PORT = 58899
DEFAULT_COLLECTOR_IP = ""
DEFAULT_DISCOVERY_TARGET = "255.255.255.255"
DEFAULT_DISCOVERY_INTERVAL = 3
DEFAULT_HEARTBEAT_INTERVAL = 60
DEFAULT_POLL_INTERVAL = 10
POLL_MODE_AUTO = "auto"
POLL_MODE_MANUAL = "manual"
DEFAULT_POLL_MODE = POLL_MODE_AUTO
DEFAULT_PROXY_CAPTURE_DURATION_MINUTES = 10
MIN_PROXY_CAPTURE_DURATION_MINUTES = 1
MAX_PROXY_CAPTURE_DURATION_MINUTES = 120

DRIVER_HINT_AUTO = "auto"
CONNECTION_TYPE_EYBOND = "eybond"
CONTROL_MODE_AUTO = "auto"
CONTROL_MODE_READ_ONLY = "read_only"
CONTROL_MODE_FULL = "full"
DEFAULT_CONTROL_MODE = CONTROL_MODE_AUTO
COLLECTOR_OPERATION_SMARTESS_AND_HA = "smartess_cloud_home_assistant"
COLLECTOR_OPERATION_HA_ONLY = "home_assistant_only"
DEFAULT_COLLECTOR_OPERATION_MODE = COLLECTOR_OPERATION_SMARTESS_AND_HA
COLLECTOR_OPERATION_MODES = {
	COLLECTOR_OPERATION_SMARTESS_AND_HA,
	COLLECTOR_OPERATION_HA_ONLY,
}

# --- Collector connection architecture axes -----------------------------------
# Three independent, explicit per-entry axes replace the old habit of inferring
# transport ownership and endpoint control from operation mode, endpoint
# hostname, peer IP, or collector type. See
# ``connection/connection_policy.py`` for the single source of truth and the
# migration mapping from the legacy fields.

# 1) connection_strategy: how Home Assistant obtains the collector TCP session.
CONF_CONNECTION_STRATEGY = "connection_strategy"
# The collector already dials Home Assistant by itself; runtime only claims or
# waits for the inbound session. It must never send a UDP callback probe, ask
# the collector to reconnect, or rewrite the endpoint for connection recovery.
CONNECTION_STRATEGY_INBOUND = "inbound"
# Home Assistant must ask the collector to dial back (UDP callback trigger) per
# explicit connection attempt, then claim the resulting inbound session.
CONNECTION_STRATEGY_CALLBACK_ON_DEMAND = "callback_on_demand"
CONNECTION_STRATEGIES = {
	CONNECTION_STRATEGY_INBOUND,
	CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
}
# Safe default: assume the collector is already connected and do not touch the
# wire on its behalf. Migration promotes cloud-primary entries to callback.
DEFAULT_CONNECTION_STRATEGY = CONNECTION_STRATEGY_INBOUND
# Diagnostic provenance for HOW the persisted connection_strategy was decided:
# behaviorally verified inbound (restart -> genuine collector dial-in with no
# callback trigger) or a verified one-shot callback attempt. Shared const layer
# so the connection policy and the onboarding verification agree on the values
# without importing each other. Never consulted for transport decisions.
CONF_CONNECTION_STRATEGY_EVIDENCE = "connection_strategy_evidence"
# The collector was restarted and dialed back in on its own, with NO callback
# trigger sent. This is the only genuine behavioral proof of `inbound`, and it
# may ONLY be recorded by the restart/reconnect verification that actually
# performed it. Never reuse it for a value the user merely asserted or picked.
# NOTE: this legacy evidence value is NOT a RecoveryContract inbound proof by
# itself -- <=v4 schemas stored neither a verification timestamp nor a strong
# identity source, so it cannot be backfilled and the contract model has no
# legacy method to backfill into (see connection/recovery_contract.py; the
# v4->v5 migration is a pure version bump, pinned by the migration tests).
CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT = "reboot_reconnect"
# LEGACY COMPATIBILITY VALUE. Historical bookkeeping: "a one-shot UDP callback
# trigger was answered on a NEW session once, during onboarding". That was an
# IDENTITY observation, never a recovery proof -- it says nothing about
# re-establishing contact after the session is lost, and it never becomes a
# RecoveryContract callback proof (connection/recovery_contract.py rejects it
# by rule). New production writers MUST NOT create this value; it exists only
# so the legacy strategy READER (connection_policy) keeps resolving entries
# that already carry it. Removal is a separate cleanup phase.
CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER = "callback_trigger"
# The user explicitly picked an observed, unclaimed strong-PN session and bound
# it to a waiting entry. That is honest provenance for `inbound` (the collector
# demonstrably dials in) but it is NOT a restart/reconnect proof -- nothing was
# restarted and nothing was triggered -- and it never becomes a RecoveryContract
# inbound proof.
CONNECTION_STRATEGY_EVIDENCE_USER_CONFIRMED_SESSION = "user_confirmed_session"

# 2) endpoint_control_policy: whether the integration may manage the endpoint.
CONF_ENDPOINT_CONTROL_POLICY = "endpoint_control_policy"
# The integration does not control the endpoint: it may read/display it and use
# inbound sessions, but must never silently write, restore, or auto-heal it.
ENDPOINT_CONTROL_EXTERNAL = "external"
# The integration previously wrote the endpoint through an explicit user action
# and may keep it aligned / restore it through further explicit actions.
ENDPOINT_CONTROL_INTEGRATION_MANAGED = "integration_managed"
ENDPOINT_CONTROL_POLICIES = {
	ENDPOINT_CONTROL_EXTERNAL,
	ENDPOINT_CONTROL_INTEGRATION_MANAGED,
}
# Safe default: never touch an endpoint the integration did not write.
DEFAULT_ENDPOINT_CONTROL_POLICY = ENDPOINT_CONTROL_EXTERNAL

# Retired compatibility field. Continuous cloud proxying was never implemented;
# new options writes keep this false. Do not use it as runtime authority.
CONF_PROXY_ENABLED = "proxy_enabled"
DEFAULT_PROXY_ENABLED = False

# Endpoint provenance for integration-managed control. The opaque "previous"
# endpoint is the existing CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT; these two
# record what the integration itself last wrote and when.
CONF_ENDPOINT_WRITTEN_VALUE = "endpoint_written_value"
CONF_ENDPOINT_WRITTEN_AT = "endpoint_written_at"

DEFAULT_COLLECTOR_ADDR = 0xFF
DEFAULT_MODBUS_DEVICE_ADDR = 1
DEFAULT_REQUEST_TIMEOUT = 5.0

SERVICE_CREATE_LOCAL_PROFILE_DRAFT = "create_local_profile_draft"
SERVICE_CREATE_LOCAL_SCHEMA_DRAFT = "create_local_schema_draft"
SERVICE_RELOAD_LOCAL_METADATA = "reload_local_metadata"
SERVICE_BIND_COLLECTOR_TO_HOME_ASSISTANT = "bind_collector_to_home_assistant"
SERVICE_APPLY_COLLECTOR_CHANGES = "apply_collector_changes"
SERVICE_REBOOT_COLLECTOR = "reboot_collector"
SERVICE_ROLLBACK_COLLECTOR_SERVER_ENDPOINT = "rollback_collector_server_endpoint"
SERVICE_SET_COLLECTOR_SERVER_ENDPOINT = "set_collector_server_endpoint"
SERVICE_START_PROXY_CAPTURE = "start_proxy_capture"
SERVICE_STOP_PROXY_CAPTURE = "stop_proxy_capture"
SERVICE_RUN_DIAGNOSTIC_COMMANDS = "run_diagnostic_commands"
