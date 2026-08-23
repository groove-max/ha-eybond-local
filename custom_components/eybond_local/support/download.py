"""Authenticated download endpoint helpers for EyeBond support artifacts."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from aiohttp import hdrs

from ..const import DOMAIN, LOCAL_DIAGNOSTIC_RUNS_DIR, LOCAL_METADATA_DIR
from .package import support_packages_root
from .proxy_capture.trace import proxy_trace_root

_DOWNLOAD_VIEWS_REGISTERED = "download_views_registered"


def support_package_authenticated_download_url(entry_id: str) -> str:
    """Return the authenticated HA API URL for one entry's latest support archive."""

    return f"/api/{DOMAIN}/support_package/{entry_id}"


def sign_support_package_download_url(
    hass: Any,
    entry_id: str,
    *,
    expiration: timedelta = timedelta(minutes=15),
) -> str:
    """Return a browser-navigable signed HA download URL for one support archive."""

    path = support_package_authenticated_download_url(entry_id)
    try:
        from homeassistant.components.http.auth import async_sign_path
    except ModuleNotFoundError:
        return path
    return _absolute_download_url(
        hass,
        async_sign_path(hass, path, expiration),
    )


def proxy_capture_authenticated_download_url(
    entry_id: str,
    filename: str,
) -> str:
    """Return the authenticated HA API URL for one proxy-capture archive."""

    return f"/api/{DOMAIN}/proxy_capture/{entry_id}/{filename}"


def sign_proxy_capture_download_url(
    hass: Any,
    entry_id: str,
    filename: str,
    *,
    expiration: timedelta = timedelta(minutes=15),
) -> str:
    """Return a browser-navigable signed URL for one proxy-capture archive."""

    path = proxy_capture_authenticated_download_url(entry_id, filename)
    try:
        from homeassistant.components.http.auth import async_sign_path
    except ModuleNotFoundError:
        return path
    signed_path = async_sign_path(hass, path, expiration)
    return _absolute_download_url(hass, signed_path)


def diagnostic_run_authenticated_download_url(
    entry_id: str,
    filename: str,
) -> str:
    """Return the authenticated HA API URL for one diagnostic result."""

    return f"/api/{DOMAIN}/diagnostic_run/{entry_id}/{filename}"


def sign_diagnostic_run_download_url(
    hass: Any,
    entry_id: str,
    filename: str,
    *,
    expiration: timedelta = timedelta(minutes=15),
) -> str:
    """Return a browser-navigable signed URL for one diagnostic result."""

    path = diagnostic_run_authenticated_download_url(entry_id, filename)
    try:
        from homeassistant.components.http.auth import async_sign_path
    except ModuleNotFoundError:
        return path
    return _absolute_download_url(hass, async_sign_path(hass, path, expiration))


def _absolute_download_url(hass: Any, signed_path: str) -> str:
    """Keep signed downloads out of Home Assistant's SPA router.

    Prefer the exact origin of the REST request that created the options-flow
    result.  Configured HA URLs are only fallbacks: an administrator may open
    the same instance through LAN, VPN, or reverse-proxy origins that are not
    recorded in ``configuration.yaml``.
    """

    try:
        from homeassistant.helpers.http import current_request
    except ModuleNotFoundError:
        request = None
    else:
        request = current_request.get()
    if request is not None:
        try:
            origin = str(request.url.origin()).strip()
        except (AttributeError, TypeError, ValueError):
            origin = ""
        if origin:
            return f"{origin.rstrip('/')}{signed_path}"

    config = getattr(hass, "config", None)
    base_url = (
        str(getattr(config, "external_url", "") or "").strip()
        or str(getattr(config, "internal_url", "") or "").strip()
    )
    if not base_url:
        return signed_path
    return f"{base_url.rstrip('/')}{signed_path}"


def resolve_support_package_download_path(
    *,
    config_dir: Path,
    entry_id: str,
    coordinator: Any,
) -> Path | None:
    """Return the current support package path if it is safe to serve."""

    values = getattr(getattr(coordinator, "data", None), "values", {}) or {}
    raw_path = str(values.get("support_package_path") or "").strip()
    if not raw_path:
        return None

    try:
        support_root = support_packages_root(Path(config_dir)).resolve()
        path = Path(raw_path).expanduser().resolve()
        path.relative_to(support_root)
    except (OSError, RuntimeError, ValueError):
        return None

    expected_prefix = f"{entry_id}_"
    if not path.name.startswith(expected_prefix) or path.suffix.lower() != ".zip":
        return None
    if not path.is_file():
        return None
    return path


def resolve_proxy_capture_download_path(
    *,
    config_dir: Path,
    entry_id: str,
    filename: str,
) -> Path | None:
    """Resolve one entry-owned proxy ZIP without exposing arbitrary files."""

    if (
        type(entry_id) is not str
        or not entry_id
        or entry_id != entry_id.strip()
        or type(filename) is not str
        or not filename
        or filename != filename.strip()
    ):
        return None
    if Path(filename).name != filename:
        return None
    if not filename.startswith(f"{entry_id}_") or not filename.endswith(".zip"):
        return None
    try:
        root = proxy_trace_root(Path(config_dir)).resolve()
        path = (root / filename).resolve()
        path.relative_to(root)
    except (OSError, RuntimeError, ValueError):
        return None
    return path if path.is_file() else None


def resolve_diagnostic_run_download_path(
    *,
    config_dir: Path,
    entry_id: str,
    filename: str,
) -> Path | None:
    """Resolve one entry-owned redacted diagnostic result."""

    if (
        type(entry_id) is not str
        or not entry_id
        or entry_id != entry_id.strip()
        or type(filename) is not str
        or not filename
        or filename != filename.strip()
    ):
        return None
    if Path(filename).name != filename:
        return None
    if (
        not filename.startswith(f"diagnostic_{entry_id}_")
        or not filename.endswith(".share.json")
    ):
        return None
    try:
        root = (
            Path(config_dir) / LOCAL_METADATA_DIR / LOCAL_DIAGNOSTIC_RUNS_DIR
        ).resolve()
        path = (root / filename).resolve()
        path.relative_to(root)
    except (OSError, RuntimeError, ValueError):
        return None
    return path if path.is_file() else None


def download_request_allowed(request: Any) -> bool:
    """Authorize a request after Home Assistant auth middleware accepted it.

    Signed browser navigation carries ``authSig`` without a bearer header.
    Normal API requests remain admin-only. Checking for ``authSig`` alone is
    unsafe because HA prefers a bearer header when both are present; a
    non-admin bearer request must not bypass the admin check by appending an
    arbitrary query parameter.
    """

    user = request["hass_user"]
    if user.is_admin:
        return True
    return hdrs.AUTHORIZATION not in request.headers and "authSig" in request.query


def async_register_download_views(hass: Any) -> bool:
    """Register the authenticated artifact download endpoints once."""

    try:
        from aiohttp import web
        from homeassistant.components.http.view import HomeAssistantView
        from homeassistant.exceptions import Unauthorized
    except ModuleNotFoundError:
        return False

    hass_data = getattr(hass, "data", None)
    if hass_data is None:
        return False
    data = hass_data.setdefault(DOMAIN, {})
    if data.get(_DOWNLOAD_VIEWS_REGISTERED):
        return False

    class EybondSupportPackageDownloadView(HomeAssistantView):
        """Serve the latest support archive for one config entry."""

        url = f"/api/{DOMAIN}/support_package/{{entry_id}}"
        name = f"api:{DOMAIN}:support_package"
        requires_auth = True

        async def get(self, request, entry_id: str):
            request_hass = request.app["hass"]
            # Normal API calls still require an admin user. Browser downloads
            # use HA signed paths: those are short-lived bearer URLs generated
            # by Home Assistant specifically for navigation/download requests,
            # and may be backed by HA's content user when generated outside an
            # HTTP/WebSocket request context.
            if not download_request_allowed(request):
                raise Unauthorized()

            entry = request_hass.config_entries.async_get_entry(entry_id)
            if entry is None:
                raise web.HTTPNotFound()

            path = resolve_support_package_download_path(
                config_dir=Path(request_hass.config.config_dir),
                entry_id=entry_id,
                coordinator=getattr(entry, "runtime_data", None),
            )
            if path is None:
                raise web.HTTPNotFound()

            return web.FileResponse(
                path,
                headers={
                    "Content-Disposition": f'attachment; filename="{path.name}"',
                },
            )

    class EybondProxyCaptureDownloadView(HomeAssistantView):
        """Serve one signed entry-owned proxy-capture archive."""

        url = f"/api/{DOMAIN}/proxy_capture/{{entry_id}}/{{filename}}"
        name = f"api:{DOMAIN}:proxy_capture"
        requires_auth = True

        async def get(self, request, entry_id: str, filename: str):
            request_hass = request.app["hass"]
            if not download_request_allowed(request):
                raise Unauthorized()
            if request_hass.config_entries.async_get_entry(entry_id) is None:
                raise web.HTTPNotFound()
            path = resolve_proxy_capture_download_path(
                config_dir=Path(request_hass.config.config_dir),
                entry_id=entry_id,
                filename=filename,
            )
            if path is None:
                raise web.HTTPNotFound()
            return web.FileResponse(
                path,
                headers={
                    "Content-Disposition": f'attachment; filename="{path.name}"',
                },
            )

    class EybondDiagnosticRunDownloadView(HomeAssistantView):
        """Serve one signed entry-owned redacted diagnostic result."""

        url = f"/api/{DOMAIN}/diagnostic_run/{{entry_id}}/{{filename}}"
        name = f"api:{DOMAIN}:diagnostic_run"
        requires_auth = True

        async def get(self, request, entry_id: str, filename: str):
            request_hass = request.app["hass"]
            if not download_request_allowed(request):
                raise Unauthorized()
            if request_hass.config_entries.async_get_entry(entry_id) is None:
                raise web.HTTPNotFound()
            path = resolve_diagnostic_run_download_path(
                config_dir=Path(request_hass.config.config_dir),
                entry_id=entry_id,
                filename=filename,
            )
            if path is None:
                raise web.HTTPNotFound()
            return web.FileResponse(
                path,
                headers={
                    "Content-Disposition": f'attachment; filename="{path.name}"',
                },
            )

    hass.http.register_view(EybondSupportPackageDownloadView())
    hass.http.register_view(EybondProxyCaptureDownloadView())
    hass.http.register_view(EybondDiagnosticRunDownloadView())
    data[_DOWNLOAD_VIEWS_REGISTERED] = True
    return True
