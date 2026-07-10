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
