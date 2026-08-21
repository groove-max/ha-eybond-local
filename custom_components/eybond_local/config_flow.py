"""Home Assistant config-flow composition root."""

from __future__ import annotations

from homeassistant.config_entries import ConfigFlow

from .flows.config.admission import CollectorAdmissionFlowMixin
from .flows.config.base import ConfigFlowBaseMixin
from .flows.config.ble import BluetoothProvisioningFlowMixin
from .flows.config.collector import SelectedCollectorFlowMixin
from .flows.config.confirmation import CollectorConfirmationFlowMixin
from .config_entry import EntryCommitFlowMixin
from .flows.config.manual import ManualCollectorFlowMixin
from .flows.config.network import ConfigNetworkFlowMixin
from .flows.config.results import ScanResultPresentationMixin
from .flows.config.scan import CollectorScanFlowMixin
from .const import DOMAIN
from .flows.common.translation import TranslationBundleMixin


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
