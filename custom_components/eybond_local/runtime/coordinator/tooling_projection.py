"""Pure support and tooling projections for the runtime coordinator."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ...build_identity import build_runtime_values

from ...const import (
    DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
    DOMAIN,
    MAX_PROXY_CAPTURE_DURATION_MINUTES,
    MIN_PROXY_CAPTURE_DURATION_MINUTES,
)
from ...support.proxy_capture import PROXY_WIRE_TRANSPARENT
from ...support.proxy_capture.trace import parse_proxy_capture_session_timestamp


def proxy_capture_state_wire_mode(state: object) -> str:
    """Return the persisted mode, defaulting only records from older builds."""

    value = getattr(state, "proxy_wire_mode", PROXY_WIRE_TRANSPARENT)
    if type(value) is not str or value != PROXY_WIRE_TRANSPARENT:
        return PROXY_WIRE_TRANSPARENT
    return value


@dataclasses.dataclass(frozen=True, slots=True)
class CloudToolEndpointContext:
    """One live, exact endpoint context shared by proxy and shadow learning."""

    current_endpoint: str
    upstream_endpoint: str
    target_endpoint: str


def bounded_shadow_learning_artifact_path(
    *,
    config_dir: Path,
    value: object,
    relative_root: Path,
) -> str:
    """Return an existing artifact path only when it stays inside its expected root."""

    normalized = str(value or "").strip()
    if not normalized:
        return ""
    path = Path(normalized)
    if not path.is_absolute():
        return ""
    root = (config_dir / relative_root).resolve()
    candidate = path.resolve()
    if candidate == root or root not in candidate.parents:
        return ""
    if not candidate.exists() or not candidate.is_file():
        return ""
    return str(candidate)


LOCALIZED_RUNTIME_TEXT: dict[str, dict[str, str]] = {
    "cloud_learning_hold_title": {
        "en": "EyeBond Local: cloud learning protection",
        "ru": "EyeBond Local: защита после облачного обучения",
        "uk": "EyeBond Local: захист після хмарного навчання",
    },
    "cloud_learning_hold_body": {
        "en": "Automatic cloud reconnection and new active learning are blocked because earlier test commands may remain queued. Do not reconnect the collector to the cloud manually or delete protection files. Local HA is not blocked, but an interrupted older run may need connection recovery. Successfully learned controls can still be applied. Download a Support Archive from Expand device support for maintainer review. Dismissing this notification does not release protection.",
        "ru": "Автоматическое подключение к облаку и новое активное обучение заблокированы: ранее отправленные тестовые команды могут оставаться в очереди. Не подключайте коллектор к облаку вручную и не удаляйте файлы защиты. Локальный HA не блокируется, но прерванное обучение старой версии может требовать восстановления связи. Успешно изученные контролы можно применить. Скачайте архив поддержки в разделе расширения поддержки устройства для проверки с разработчиком. Закрытие уведомления не снимает защиту.",
        "uk": "Автоматичне підключення до хмари та нове активне навчання заблоковані: раніше надіслані тестові команди можуть залишатися в черзі. Не підключайте колектор до хмари вручну й не видаляйте файли захисту. Локальний HA не блокується, але перерване навчання старої версії може потребувати відновлення зв’язку. Успішно вивчені контроли можна застосувати. Завантажте архів підтримки в розділі розширення підтримки пристрою для перевірки з розробником. Закриття сповіщення не знімає захист.",
    },
    "proxy_capture_notification_title": {
        "en": "EyeBond Local Collector Capture",
        "ru": "EyeBond Local: захват трафика коллектора",
        "uk": "EyeBond Local: захоплення трафіку колектора",
    },
    "proxy_capture_notification_body": {
        "en": 'Your collector traffic capture is ready.\n\n<a href="{download_url}" target="_blank" rel="noopener">Download capture bundle</a>',
        "ru": 'Захват трафика коллектора готов.\n\n<a href="{download_url}" target="_blank" rel="noopener">Скачать архив захвата</a>',
        "uk": 'Захоплення трафіку колектора готове.\n\n<a href="{download_url}" target="_blank" rel="noopener">Завантажити архів захоплення</a>',
    },
    "proxy_capture_notification_body_no_link": {
        "en": "Your collector traffic capture is ready.\n\nSaved archive: {saved_path}",
        "ru": "Захват трафика коллектора готов.\n\nСохраненный архив: {saved_path}",
        "uk": "Захоплення трафіку колектора готове.\n\nЗбережений архів: {saved_path}",
    },
    "proxy_capture_restore_unconfirmed_title": {
        "en": "EyeBond Local Collector Restore Needs Attention",
        "ru": "EyeBond Local: проверьте восстановление коллектора",
        "uk": "EyeBond Local: перевірте відновлення колектора",
    },
    "proxy_capture_restore_unconfirmed_body": {
        "en": "The proxy capture stopped, but restoring the collector's previous endpoint was not confirmed. The collector may still point at Home Assistant. If the vendor cloud no longer sees the collector, use the \"Restore previous collector endpoint\" action after the collector reconnects.",
        "ru": "Захват трафика остановлен, но восстановление предыдущего адреса коллектора не подтверждено. Коллектор может всё ещё указывать на Home Assistant. Если облако производителя больше не видит коллектор, после его повторного подключения воспользуйтесь действием «Восстановить предыдущий адрес коллектора».",
        "uk": "Захоплення трафіку зупинено, але відновлення попередньої адреси колектора не підтверджено. Колектор може все ще вказувати на Home Assistant. Якщо хмара виробника більше не бачить колектор, після повторного підключення скористайтеся дією «Відновити попередню адресу колектора».",
    },
    "support_archive_notification_title": {
        "en": "EyeBond Local Support Archive",
        "ru": "EyeBond Local: архив поддержки",
        "uk": "EyeBond Local: архів підтримки",
    },
    "support_archive_notification_body": {
        "en": "Your support archive is ready.\n\n[Download support archive]({download_url})",
        "ru": "Архив поддержки готов.\n\n[Скачать архив поддержки]({download_url})",
        "uk": "Архів підтримки готовий.\n\n[Завантажити архів підтримки]({download_url})",
    },
    "poll_interval_high_utilization_title": {
        "en": "EyeBond Local polling interval is tight",
        "ru": "EyeBond Local: интервал опроса близок к пределу",
        "uk": "EyeBond Local: інтервал опитування близький до межі",
    },
    "poll_interval_high_utilization_body": {
        "en": "The device polling cycle is using about {utilization_percent}% of the configured {poll_interval}s interval. If updates are delayed, increase the manual polling interval or switch Sensor refresh mode to Automatic. Recommended minimum for this device is about {recommended_interval}s.",
        "ru": "Цикл опроса устройства использует около {utilization_percent}% настроенного интервала {poll_interval}s. Если обновления задерживаются, увеличьте ручной интервал опроса или переключите режим обновления сенсоров на автоматический. Рекомендуемый минимум для этого устройства — около {recommended_interval}s.",
        "uk": "Цикл опитування пристрою використовує близько {utilization_percent}% налаштованого інтервалу {poll_interval}s. Якщо оновлення затримуються, збільште ручний інтервал опитування або перемкніть режим оновлення сенсорів на автоматичний. Рекомендований мінімум для цього пристрою — близько {recommended_interval}s.",
    },
    "inverter_protocol_ambiguous_title": {
        "en": "EyeBond Local needs an inverter protocol choice",
        "ru": "EyeBond Local: нужно выбрать протокол инвертора",
        "uk": "EyeBond Local: потрібно вибрати протокол інвертора",
    },
    "inverter_protocol_ambiguous_body": {
        "en": "The collector is connected, but the inverter answered using {count} supported protocols. Open this EyeBond Local entry's settings and choose **Inverter protocol**. Inverter entities will be created after the selected protocol is confirmed on the active connection.",
        "ru": "Коллектор подключён, но инвертор ответил по {count} поддерживаемым протоколам. Откройте настройки этой записи EyeBond Local и выберите **Протокол инвертора**. Сущности инвертора появятся после проверки выбранного протокола через активное подключение.",
        "uk": "Колектор підключено, але інвертор відповів через {count} підтримувані протоколи. Відкрийте налаштування цього запису EyeBond Local і виберіть **Протокол інвертора**. Сутності інвертора з’являться після перевірки вибраного протоколу через активне підключення.",
    },
}


def runtime_language(hass) -> str:
    language = str(getattr(getattr(hass, "config", None), "language", "en") or "en").lower()
    return language.split("-", 1)[0]


def localized_runtime_text(hass, key: str, **placeholders: Any) -> str:
    templates = LOCALIZED_RUNTIME_TEXT.get(key, {})
    template = templates.get(runtime_language(hass), templates.get("en", ""))
    if not template:
        return ""
    return template.format(**placeholders)


def proxy_capture_notification_id(entry_id: str, bundle_path: Path | str) -> str:
    stem = Path(str(bundle_path or "capture")).stem or "capture"
    return f"{DOMAIN}_proxy_capture_{entry_id}_{stem}"


def _package_dir() -> Path:
    return Path(__file__).resolve().parents[2]


def integration_build_runtime_values() -> dict[str, object]:
    """Return support-facing package/build diagnostics for the loaded code."""

    return build_runtime_values(_package_dir())


def coerce_proxy_capture_duration_minutes(value: object) -> int:
    try:
        minutes = int(round(float(value)))
    except (TypeError, ValueError):
        minutes = DEFAULT_PROXY_CAPTURE_DURATION_MINUTES
    return max(
        MIN_PROXY_CAPTURE_DURATION_MINUTES,
        min(MAX_PROXY_CAPTURE_DURATION_MINUTES, minutes),
    )


def proxy_capture_remaining_seconds(expires_at: object) -> int:
    deadline = parse_proxy_capture_session_timestamp(str(expires_at or ""))
    if deadline is None:
        return 0
    return max(0, int((deadline - datetime.now(timezone.utc)).total_seconds()))


PROXY_CAPTURE_TRANSIENT_RUNTIME_KEYS = (
    "proxy_capture_session_status",
    "proxy_capture_session_started_at",
    "proxy_capture_session_expires_at",
    "proxy_capture_session_anonymized",
    "proxy_capture_remaining_seconds",
    "proxy_capture_remaining_minutes",
)


__all__ = [
    "CloudToolEndpointContext",
    "PROXY_CAPTURE_TRANSIENT_RUNTIME_KEYS",
    "bounded_shadow_learning_artifact_path",
    "coerce_proxy_capture_duration_minutes",
    "integration_build_runtime_values",
    "localized_runtime_text",
    "proxy_capture_notification_id",
    "proxy_capture_remaining_seconds",
    "proxy_capture_state_wire_mode",
]
