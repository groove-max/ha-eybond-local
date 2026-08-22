"""Shared collector transport API composition.

Concrete socket/session ownership lives in the sibling transport modules.  This
module intentionally contains no second implementation or mutable authority.
"""

from .common import (
    CollectorAtTransport,
    CollectorListenerBindError,
    CollectorTransport,
    _BACKGROUND_TASKS,
    _WRITER_CLOSE_TIMEOUT,
    _collector_pn_from_initial_chunk,
    _finish_cleanup_on_cancel,
    _parse_fc2_collector_pn,
)
from .connections import _CollectorAtConnection, _CollectorConnection
from .listener import (
    _LISTENERS,
    _PendingCollectorSocket,
    _SharedEybondListener,
    _acquire_shared_at_listener,
    _acquire_shared_listener,
    _acquire_shared_payload_listener,
    _release_shared_listener,
)
from .proxy import SharedProxyCaptureRoute
from .shared_at import SharedCollectorAtTransport
from .shared_framed import SharedEybondTransport

__all__ = [
    "CollectorAtTransport",
    "CollectorListenerBindError",
    "CollectorTransport",
    "SharedCollectorAtTransport",
    "SharedEybondTransport",
    "SharedProxyCaptureRoute",
]
