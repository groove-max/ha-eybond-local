"""Home Assistant config-flow composition root."""

from __future__ import annotations

from homeassistant.config_entries import ConfigFlow

from .config_admission import CollectorAdmissionFlowMixin
from .config_base import ConfigFlowBaseMixin
from .config_ble import BluetoothProvisioningFlowMixin
from .config_collector import SelectedCollectorFlowMixin
from .config_confirmation import CollectorConfirmationFlowMixin
from .config_entry import EntryCommitFlowMixin
from .config_manual import ManualCollectorFlowMixin
from .config_network import ConfigNetworkFlowMixin
from .config_results import ScanResultPresentationMixin
from .config_scan import CollectorScanFlowMixin
from .const import DOMAIN
from .flow_translation import TranslationBundleMixin


class EybondLocalConfigFlow(
    CollectorAdmissionFlowMixin,
    CollectorScanFlowMixin,
    BluetoothProvisioningFlowMixin,
    CollectorConfirmationFlowMixin,
    ManualCollectorFlowMixin,
    EntryCommitFlowMixin,
    SelectedCollectorFlowMixin,
    ConfigNetworkFlowMixin,
    ScanResultPresentationMixin,
    ConfigFlowBaseMixin,
    TranslationBundleMixin,
    ConfigFlow,
    domain=DOMAIN,
):
    """Create a config entry for an inverter behind an EyeBond collector."""

    VERSION = 5


__all__ = ["EybondLocalConfigFlow"]
