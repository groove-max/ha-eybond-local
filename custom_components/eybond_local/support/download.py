"""Authenticated download endpoint helpers for support archives."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from aiohttp import hdrs

from ..const import DOMAIN
from .package import support_packages_root
from .proxy_trace import proxy_trace_root

_SUPPORT_PACKAGE_DOWNLOAD_VIEW_REGISTERED = "support_package_download_view_registered"


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
    return async_sign_path(hass, path, expiration)


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


def support_package_download_request_allowed(request: Any) -> bool:
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


def async_register_support_package_download_view(hass: Any) -> bool:
    """Register the authenticated support package download endpoint once."""

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
    if data.get(_SUPPORT_PACKAGE_DOWNLOAD_VIEW_REGISTERED):
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
            if not support_package_download_request_allowed(request):
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
            if not support_package_download_request_allowed(request):
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

    hass.http.register_view(EybondSupportPackageDownloadView())
    hass.http.register_view(EybondProxyCaptureDownloadView())
    data[_SUPPORT_PACKAGE_DOWNLOAD_VIEW_REGISTERED] = True
    return True
