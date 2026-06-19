"""Helpers for building one end-to-end project quality gate."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
TOOLS_DIR = REPO_ROOT / "tools"
DOCS_DIR = REPO_ROOT / "docs"
GENERATED_DOCS_DIR = DOCS_DIR / "generated"
PACKAGE_DIR = REPO_ROOT / "custom_components" / "eybond_local"


@dataclass(frozen=True, slots=True)
class GeneratedExport:
    """One generated docs export that can be refreshed or checked."""

    key: str
    tool_path: Path
    output_path: Path

    def command(self, python_executable: str, *, check: bool) -> tuple[str, ...]:
        """Build the command line for this export."""

        args: list[str] = [
            python_executable,
            str(self.tool_path),
            "--format",
            "markdown",
        ]
        args.extend(["--output", str(self.output_path)])
        if check:
            args.append("--check")
        return tuple(args)


@dataclass(frozen=True, slots=True)
class QualityGateStep:
    """One command that belongs to the project quality gate."""

    key: str
    title: str
    command: tuple[str, ...]


def generated_exports() -> tuple[GeneratedExport, ...]:
    """Return the public generated docs exports that stay checked in."""

    return (
        GeneratedExport(
            key="model_catalog",
            tool_path=TOOLS_DIR / "model_catalog.py",
            output_path=GENERATED_DOCS_DIR / "INVERTER_MODEL_CATALOG.generated.md",
        ),
    )


def build_quality_gate_steps(
    *,
    python_executable: str = "python3",
    refresh_generated: bool = False,
) -> tuple[QualityGateStep, ...]:
    """Return the ordered quality gate commands for this repository."""

    steps: list[QualityGateStep] = [
        QualityGateStep(
            key="validate_profiles",
            title="Validate declarative profiles",
            command=(python_executable, str(TOOLS_DIR / "validate_profiles.py")),
        ),
        QualityGateStep(
            key="validate_model_catalog",
            title="Validate inverter model catalog",
            command=(python_executable, str(TOOLS_DIR / "model_catalog.py"), "validate"),
        ),
        QualityGateStep(
            key="unit_tests",
            title="Run unit tests",
            command=(python_executable, "-m", "unittest", "discover", "-s", "tests", "-v"),
        ),
        QualityGateStep(
            key="compileall",
            title="Compile runtime and tools",
            command=(
                python_executable,
                "-m",
                "compileall",
                str(PACKAGE_DIR),
                str(TOOLS_DIR),
            ),
        ),
    ]

    for export in generated_exports():
        if refresh_generated:
            steps.append(
                QualityGateStep(
                    key=f"refresh_{export.key}",
                    title=f"Refresh generated {export.key}",
                    command=export.command(python_executable, check=False),
                )
            )
        steps.append(
            QualityGateStep(
                key=f"check_{export.key}",
                title=f"Check generated {export.key}",
                command=export.command(python_executable, check=True),
            )
        )

    return tuple(steps)
