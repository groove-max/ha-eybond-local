#!/usr/bin/env python3
"""Run validation at the depth appropriate for the current change."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import py_compile
import subprocess
import sys
from time import perf_counter


REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = REPO_ROOT / "custom_components" / "eybond_local"
TEST_ROOT = REPO_ROOT / "tests"
HA_TEST_ROOT = REPO_ROOT / "tests_ha"

_FAMILY_TESTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "custom_components/eybond_local/runtime/coordinator",
        (
            "test_coordinator_device_hierarchy.py",
            "test_coordinator_module_boundaries.py",
        ),
    ),
    (
        "custom_components/eybond_local/runtime/hub",
        ("test_hub.py", "test_hub_module_boundaries.py"),
    ),
    (
        "custom_components/eybond_local/runtime/link",
        ("test_runtime_link.py", "test_link_module_boundaries.py"),
    ),
    (
        "custom_components/eybond_local/collector/transport",
        ("test_shared_transport.py", "test_transport_module_boundaries.py"),
    ),
    (
        "custom_components/eybond_local/connection/recovery",
        (
            "test_callback_recovery.py",
            "test_recovery_contract.py",
            "test_recovery_verification_module_boundaries.py",
        ),
    ),
    (
        "custom_components/eybond_local/flows/config",
        ("test_config_flow.py", "test_flow_module_boundaries.py"),
    ),
    (
        "custom_components/eybond_local/flows/options",
        ("test_config_flow.py", "test_flow_module_boundaries.py"),
    ),
    (
        "custom_components/eybond_local/config_",
        ("test_config_flow.py", "test_flow_module_boundaries.py"),
    ),
    (
        "custom_components/eybond_local/options_",
        ("test_config_flow.py", "test_flow_module_boundaries.py"),
    ),
    (
        "custom_components/eybond_local/flow_translations/",
        ("test_translations.py",),
    ),
    (
        "custom_components/eybond_local/translations/",
        ("test_translations.py",),
    ),
    (
        "custom_components/eybond_local/strings.json",
        ("test_translations.py",),
    ),
    (
        "custom_components/eybond_local/support/proxy",
        (
            "test_proxy_capture.py",
            "test_proxy_session.py",
            "test_proxy_trace.py",
            "test_support_package_boundaries.py",
        ),
    ),
    (
        "custom_components/eybond_local/support/shadow",
        (
            "test_shadow_learning_backend.py",
            "test_shadow_learning_runtime.py",
            "test_shadow_learning_runtime_boundary.py",
            "test_support_package_boundaries.py",
        ),
    ),
)

# Foundational modules whose behavioral tests do not share the production file
# name. Keep this table narrow: family mappings cover composition roots, while
# these entries prevent a cheap ``affected`` run from silently missing a typed
# boundary or neutral wire contract.
_EXACT_TESTS: dict[str, tuple[str, ...]] = {
    "custom_components/eybond_local/support/shadow_learning/read_evidence.py": (
        "test_read_learning_binder.py",
        "test_shadow_learning_backend.py",
        "test_shadow_learning_overlay_generator.py",
        "test_device_scoped_overlay_activation.py",
        "test_effective_metadata.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/read_learning_binder.py": (
        "test_read_learning_binder.py",
        "test_shadow_learning_overlay_generator.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/shadow_learning/overlay_generator.py": (
        "test_shadow_learning_overlay_generator.py",
        "test_device_scoped_overlay_activation.py",
        "test_effective_metadata.py",
        "test_config_flow.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/device_scoped_overlay.py": (
        "test_device_scoped_overlay_activation.py",
        "test_effective_metadata.py",
        "test_config_flow.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/metadata/effective_metadata.py": (
        "test_effective_metadata.py",
        "test_device_scoped_overlay_activation.py",
        "test_coordinator_device_hierarchy.py",
        "test_config_flow.py",
    ),
    "custom_components/eybond_local/models.py": (
        "test_runtime_snapshot.py",
        "test_typed_telemetry.py",
    ),
    "custom_components/eybond_local/telemetry.py": (
        "test_typed_telemetry.py",
        "test_driver_read_contract.py",
        "test_canonical_telemetry.py",
        "test_support_bundle.py",
    ),
    "custom_components/eybond_local/canonical_telemetry.py": (
        "test_canonical_telemetry.py",
        "test_derived_energy.py",
        "test_typed_telemetry.py",
    ),
    "custom_components/eybond_local/drivers/read_result.py": (
        "test_driver_read_contract.py",
        "test_typed_telemetry.py",
    ),
    "custom_components/eybond_local/drivers/local_register_evidence.py": (
        "test_local_register_evidence.py",
        "test_local_register_series.py",
        "test_cloud_local_history_correlation.py",
        "test_driver_local_register_evidence.py",
        "test_hub.py",
        "test_config_flow.py",
        "test_shadow_learning_support_package.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/drivers/base.py": (
        "test_driver_local_register_evidence.py",
        "test_driver_read_contract.py",
        "test_hub.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/drivers/smg.py": (
        "test_smg_driver.py",
        "test_driver_local_register_evidence.py",
    ),
    "custom_components/eybond_local/drivers/must.py": (
        "test_must_driver.py",
        "test_driver_local_register_evidence.py",
    ),
    "custom_components/eybond_local/drivers/srne.py": (
        "test_srne_driver.py",
        "test_driver_local_register_evidence.py",
    ),
    "custom_components/eybond_local/drivers/modbus_catalog.py": (
        "test_modbus_catalog_driver.py",
        "test_driver_local_register_evidence.py",
    ),
    "custom_components/eybond_local/drivers/smartess_local.py": (
        "test_smartess_local_driver.py",
        "test_driver_local_register_evidence.py",
    ),
    "custom_components/eybond_local/collector/at_runtime.py": (
        "test_collector_at.py",
        "test_collector_metadata.py",
        "test_collector_metadata_architecture.py",
        "test_collector_virtual_bridge.py",
    ),
    "custom_components/eybond_local/collector/parameter_registry.py": (
        "test_collector_parameter_registry.py",
        "test_collector_metadata.py",
        "test_collector_metadata_architecture.py",
    ),
    "custom_components/eybond_local/collector/collector_wire.py": (
        "test_collector_management.py",
        "test_smartess_local.py",
        "test_shadow_learning_proxy.py",
        "test_shadow_learning_proxy_e2e.py",
        "test_fake_collector.py",
        "test_config_flow.py",
        "test_collector_metadata_architecture.py",
    ),
    "custom_components/eybond_local/collector/smartess_local.py": (
        "test_smartess_local.py",
        "test_collector_parameter_registry.py",
    ),
    "custom_components/eybond_local/dessmonitor_cloud.py": (
        "test_dessmonitor_cloud.py",
        "test_dessmonitor_collection.py",
        "test_dessmonitor_history.py",
        "test_dessmonitor_history_resolution.py",
        "test_cloud_local_history_correlation.py",
        "test_dessmonitor_time_basis.py",
        "test_dessmonitor_learning.py",
        "test_dessmonitor_semantics.py",
        "test_cloud_semantic_evidence.py",
        "test_cloud_local_coverage.py",
        "test_cloud_learning_engines.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/dessmonitor_collection.py": (
        "test_dessmonitor_collection.py",
        "test_dessmonitor_cloud.py",
        "test_dessmonitor_history.py",
        "test_dessmonitor_time_basis.py",
        "test_dessmonitor_history_resolution.py",
        "test_dessmonitor_learning.py",
        "test_cloud_learning_engines.py",
        "test_config_flow.py",
        "test_shadow_learning_support_package.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/dessmonitor_history.py": (
        "test_dessmonitor_collection.py",
        "test_dessmonitor_history.py",
        "test_dessmonitor_history_resolution.py",
        "test_cloud_local_history_correlation.py",
        "test_dessmonitor_cloud.py",
        "test_cloud_learning_engines.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/dessmonitor_time_basis.py": (
        "test_dessmonitor_collection.py",
        "test_dessmonitor_time_basis.py",
        "test_dessmonitor_history.py",
        "test_dessmonitor_history_resolution.py",
        "test_cloud_local_history_correlation.py",
        "test_dessmonitor_cloud.py",
        "test_cloud_learning_engines.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/dessmonitor_history_resolution.py": (
        "test_dessmonitor_collection.py",
        "test_dessmonitor_history_resolution.py",
        "test_cloud_local_history_correlation.py",
        "test_dessmonitor_history.py",
        "test_dessmonitor_time_basis.py",
        "test_dessmonitor_cloud.py",
        "test_cloud_learning_engines.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/drivers/local_register_series.py": (
        "test_local_register_series.py",
        "test_local_register_collection.py",
        "test_local_register_evidence.py",
        "test_cloud_local_history_correlation.py",
        "test_driver_local_register_evidence.py",
        "test_config_flow.py",
        "test_shadow_learning_support_package.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/local_register_collection.py": (
        "test_local_register_collection.py",
        "test_local_register_series.py",
        "test_coordinator_device_hierarchy.py",
        "test_config_flow.py",
        "test_cloud_learning_engines.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/cloud_local_history_correlation.py": (
        "test_cloud_local_history_correlation.py",
        "test_dessmonitor_history_resolution.py",
        "test_local_register_series.py",
        "test_cloud_semantic_evidence.py",
        "test_cloud_learning_engines.py",
        "test_config_flow.py",
        "test_shadow_learning_support_package.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/cloud_history_evidence.py": (
        "test_cloud_history_evidence.py",
        "test_smartess_history.py",
        "test_smartess_read_only.py",
        "test_dessmonitor_learning.py",
        "test_cloud_local_history_correlation.py",
        "test_config_flow.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/smartess_history.py": (
        "test_smartess_history.py",
        "test_smartess_read_only.py",
        "test_cloud_history_evidence.py",
        "test_cloud_local_history_correlation.py",
        "test_cloud_learning_engines.py",
        "test_config_flow.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/smartess_read_only.py": (
        "test_smartess_read_only.py",
        "test_smartess_history.py",
        "test_cloud_history_evidence.py",
        "test_cloud_learning_engines.py",
        "test_config_flow.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/cloud_local_history_representability.py": (
        "test_cloud_local_history_correlation.py",
        "test_local_register_series.py",
        "test_cloud_semantic_evidence.py",
        "test_coordinator_device_hierarchy.py",
        "test_config_flow.py",
        "test_shadow_learning_support_package.py",
        "test_translations.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/cloud_local_history_draft.py": (
        "test_cloud_local_history_correlation.py",
        "test_config_flow.py",
        "test_shadow_learning_support_package.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/cloud_local_history_draft_writer.py": (
        "test_cloud_local_history_correlation.py",
        "test_config_flow.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/flows/options/shadow_inactive_draft.py": (
        "test_cloud_local_history_correlation.py",
        "test_config_flow.py",
        "test_cloud_evidence_architecture.py",
        "test_flow_module_boundaries.py",
    ),
    "custom_components/eybond_local/support/cloud_semantic_evidence.py": (
        "test_cloud_semantic_evidence.py",
        "test_cloud_local_coverage.py",
        "test_cloud_local_history_correlation.py",
        "test_dessmonitor_semantics.py",
        "test_dessmonitor_learning.py",
        "test_config_flow.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/cloud_local_coverage.py": (
        "test_cloud_local_coverage.py",
        "test_cloud_semantic_evidence.py",
        "test_typed_telemetry.py",
        "test_config_flow.py",
        "test_shadow_learning_support_package.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/dessmonitor_semantics.py": (
        "test_cloud_semantic_evidence.py",
        "test_dessmonitor_semantics.py",
        "test_dessmonitor_learning.py",
        "test_config_flow.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/support/dessmonitor_learning.py": (
        "test_dessmonitor_collection.py",
        "test_cloud_semantic_evidence.py",
        "test_dessmonitor_semantics.py",
        "test_dessmonitor_learning.py",
        "test_config_flow.py",
        "test_shadow_learning_support_package.py",
        "test_cloud_evidence_architecture.py",
    ),
    "custom_components/eybond_local/flows/options/shadow_metadata_review.py": (
        "test_config_flow.py",
        "test_cloud_local_history_correlation.py",
        "test_shadow_learning_support_package.py",
        "test_translations.py",
        "test_flow_module_boundaries.py",
        "test_cloud_evidence_architecture.py",
    ),
}


class ValidationError(RuntimeError):
    """A validation stage could not complete successfully."""


def _run(command: tuple[str, ...], *, cwd: Path = REPO_ROOT) -> None:
    started = perf_counter()
    print(f"+ {' '.join(command)}", flush=True)
    completed = subprocess.run(command, cwd=cwd, check=False)
    duration = perf_counter() - started
    if completed.returncode:
        raise ValidationError(
            f"command failed with exit code {completed.returncode}: {' '.join(command)}"
        )
    print(f"  OK ({duration:.1f}s)", flush=True)


def changed_paths(base: str) -> tuple[Path, ...]:
    """Return tracked changes against *base* plus untracked repository files."""

    tracked = subprocess.run(
        ("git", "diff", "--name-only", "--diff-filter=ACMR", base, "--"),
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    untracked = subprocess.run(
        ("git", "ls-files", "--others", "--exclude-standard"),
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    return tuple(
        sorted(
            {
                Path(value)
                for value in (*tracked, *untracked)
                if value and (REPO_ROOT / value).is_file()
            },
            key=lambda path: path.as_posix(),
        )
    )


def affected_test_files(paths: tuple[Path, ...]) -> tuple[Path, ...]:
    """Map changed paths to the smallest useful stub-based regression set."""

    selected: set[Path] = set()
    production_changed = False
    for path in paths:
        value = path.as_posix()
        if value.startswith("tests/test_") and path.suffix == ".py":
            selected.add(TEST_ROOT / path.name)
            continue
        if not value.startswith("custom_components/eybond_local/"):
            continue

        production_changed = True
        direct = TEST_ROOT / f"test_{path.stem}.py"
        if direct.is_file():
            selected.add(direct)
        selected.update(
            TEST_ROOT / name for name in _EXACT_TESTS.get(value, ())
        )
        for prefix, test_names in _FAMILY_TESTS:
            if value.startswith(prefix):
                selected.update(TEST_ROOT / name for name in test_names)

    if production_changed:
        selected.add(TEST_ROOT / "test_cross_layer_architecture.py")
        selected.add(TEST_ROOT / "test_integration_module_boundaries.py")
    return tuple(sorted((path for path in selected if path.is_file()), key=str))


def print_validation_plan(base: str) -> None:
    """Print changed files and the affected test selection without running it."""

    paths = changed_paths(base)
    tests = affected_test_files(paths)
    print(f"Changed files ({len(paths)}):")
    for path in paths:
        print(f"  {path.as_posix()}")
    print(f"Affected unit files ({len(tests)}):")
    for path in tests:
        print(f"  {path.relative_to(REPO_ROOT).as_posix()}")


def _validate_untracked_whitespace(paths: tuple[Path, ...]) -> None:
    for relative in paths:
        path = REPO_ROOT / relative
        if path.suffix not in {".py", ".json", ".md", ".yaml", ".yml"}:
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError as exc:
            raise ValidationError(f"non-UTF-8 text file: {relative}") from exc
        offenders = [index for index, line in enumerate(lines, 1) if line.rstrip() != line]
        if offenders:
            raise ValidationError(
                f"trailing whitespace in {relative}: lines {', '.join(map(str, offenders[:10]))}"
            )


def run_fast(base: str) -> tuple[Path, ...]:
    """Run checks that should be cheap enough after every edit."""

    paths = changed_paths(base)
    _run(("git", "diff", "--check", base, "--"))
    _validate_untracked_whitespace(paths)

    for relative in paths:
        path = REPO_ROOT / relative
        if path.suffix == ".py":
            try:
                py_compile.compile(str(path), doraise=True)
            except py_compile.PyCompileError as exc:
                raise ValidationError(str(exc)) from exc
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text(encoding="utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValidationError(f"invalid JSON in {relative}: {exc}") from exc

    print(f"Fast validation passed for {len(paths)} changed file(s).")
    return paths


def run_affected(base: str) -> None:
    """Run fast checks, cheap catalogs, and tests selected from changed files."""

    paths = run_fast(base)
    _run((sys.executable, "tools/validate_profiles.py"))
    _run((sys.executable, "tools/model_catalog.py", "validate"))
    tests = affected_test_files(paths)
    if not tests:
        print("No affected unit tests were selected.")
        return
    modules = tuple(path.stem for path in tests)
    print("Affected tests: " + ", ".join(path.name for path in tests))
    _run((sys.executable, "-m", "unittest", "-v", *modules), cwd=TEST_ROOT)


def run_ha(python_executable: str) -> None:
    """Run the real Home Assistant lifecycle suite in one prepared interpreter."""

    _run(
        (
            python_executable,
            "-m",
            "pytest",
            "-c",
            "tests_ha/pytest.ini",
            "tests_ha",
            "-q",
        )
    )


def _parse_lane(value: str) -> tuple[str, str]:
    lane, separator, executable = value.partition("=")
    if not separator or not lane or not executable:
        raise argparse.ArgumentTypeError("lane must use VERSION=/path/to/python")
    return lane, executable


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        choices=("plan", "fast", "affected", "unit", "ha", "release"),
        help="validation depth",
    )
    parser.add_argument(
        "--base",
        default="HEAD",
        help="Git revision used to select changed files (default: HEAD)",
    )
    parser.add_argument(
        "--ha-python",
        default=sys.executable,
        help="prepared Python interpreter for mode=ha",
    )
    parser.add_argument(
        "--ha-lane",
        action="append",
        default=[],
        type=_parse_lane,
        metavar="VERSION=PYTHON",
        help="required HA lane for mode=release; may be repeated",
    )
    args = parser.parse_args()

    try:
        if args.mode == "plan":
            print_validation_plan(args.base)
        elif args.mode == "fast":
            run_fast(args.base)
        elif args.mode == "affected":
            run_affected(args.base)
        elif args.mode == "unit":
            _run((sys.executable, "tools/quality_gate.py"))
        elif args.mode == "ha":
            run_ha(args.ha_python)
        else:
            lanes = list(args.ha_lane)
            if not lanes:
                for version, variable in (
                    ("2026.2", "EYBOND_HA_2026_2_PYTHON"),
                    ("2026.7", "EYBOND_HA_2026_7_PYTHON"),
                ):
                    executable = os.environ.get(variable, "")
                    if executable:
                        lanes.append((version, executable))
            if {version for version, _executable in lanes} != {"2026.2", "2026.7"}:
                raise ValidationError(
                    "release mode requires exactly the 2026.2 and 2026.7 HA lanes; "
                    "pass --ha-lane twice or set EYBOND_HA_*_PYTHON"
                )
            _run((sys.executable, "tools/quality_gate.py"))
            for version, executable in lanes:
                print(f"HA compatibility lane {version}")
                run_ha(executable)
    except (OSError, subprocess.CalledProcessError, ValidationError) as exc:
        print(f"Validation failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
