"""Constants for the EyeBond Local integration."""

DOMAIN = "eybond_local"
PLATFORMS: list[str] = ["sensor", "binary_sensor", "number", "select", "switch", "button"]
LOCAL_METADATA_DIR = "eybond_local"
LOCAL_PROFILES_DIR = "profiles"
LOCAL_REGISTER_SCHEMAS_DIR = "register_schemas"
LOCAL_CLOUD_EVIDENCE_DIR = "cloud_evidence"
LOCAL_SUPPORT_BUNDLES_DIR = "support_bundles"
LOCAL_SUPPORT_PACKAGES_DIR = "support_packages"
BUILTIN_SCHEMA_PREFIX = "builtin:"

CONF_SERVER_IP = "server_ip"
CONF_ADVERTISED_SERVER_IP = "advertised_server_ip"
CONF_COLLECTOR_IP = "collector_ip"
CONF_COLLECTOR_PN = "collector_pn"
CONF_CONNECTION_TYPE = "connection_type"
CONF_CONNECTION_MODE = "connection_mode"
CONF_CONTROL_MODE = "control_mode"
CONF_DETECTION_CONFIDENCE = "detection_confidence"
CONF_DETECTED_MODEL = "detected_model"
CONF_DETECTED_SERIAL = "detected_serial"
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
CONF_DRIVER_HINT = "driver_hint"

DEFAULT_TCP_PORT = 8899
DEFAULT_UDP_PORT = 58899
DEFAULT_COLLECTOR_IP = ""
DEFAULT_DISCOVERY_TARGET = "255.255.255.255"
DEFAULT_DISCOVERY_INTERVAL = 3
DEFAULT_HEARTBEAT_INTERVAL = 60
DEFAULT_POLL_INTERVAL = 10

DRIVER_HINT_AUTO = "auto"
CONNECTION_TYPE_EYBOND = "eybond"
CONTROL_MODE_AUTO = "auto"
CONTROL_MODE_READ_ONLY = "read_only"
CONTROL_MODE_FULL = "full"
DEFAULT_CONTROL_MODE = CONTROL_MODE_AUTO

DEFAULT_COLLECTOR_ADDR = 0xFF
DEFAULT_MODBUS_DEVICE_ADDR = 1
DEFAULT_REQUEST_TIMEOUT = 5.0

SERVICE_CREATE_LOCAL_PROFILE_DRAFT = "create_local_profile_draft"
SERVICE_CREATE_LOCAL_SCHEMA_DRAFT = "create_local_schema_draft"
SERVICE_RELOAD_LOCAL_METADATA = "reload_local_metadata"
