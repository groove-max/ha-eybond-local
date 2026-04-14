"""Data models for declarative register schemas."""

from __future__ import annotations

from dataclasses import dataclass

from ..models import (
    BinarySensorDescription,
    MeasurementDescription,
    RegisterValueSpec,
)


@dataclass(frozen=True, slots=True)
class RegisterBlockLayout:
    """One contiguous register block layout."""

    key: str
    start: int
    count: int


@dataclass(frozen=True, slots=True)
class RegisterSchemaMetadata:
    """Declarative register schema loaded from JSON."""

    key: str
    title: str
    driver_key: str
    protocol_family: str
    source_name: str
    source_path: str
    source_scope: str
    blocks: tuple[RegisterBlockLayout, ...]
    spec_sets: dict[str, tuple[RegisterValueSpec, ...]]
    enum_tables: dict[str, dict[int | str, str]]
    bit_labels: dict[str, dict[int, str]]
    scalar_registers: dict[str, int]
    measurement_descriptions: tuple[MeasurementDescription, ...]
    binary_sensor_descriptions: tuple[BinarySensorDescription, ...]

    def block(self, block_key: str) -> RegisterBlockLayout:
        """Return one named register block."""

        for block in self.blocks:
            if block.key == block_key:
                return block
        raise KeyError(block_key)

    def spec_set(self, set_key: str) -> tuple[RegisterValueSpec, ...]:
        """Return one named register spec set."""

        try:
            return self.spec_sets[set_key]
        except KeyError as exc:
            raise KeyError(set_key) from exc

    def enum_map_for(self, enum_key: str) -> dict[int | str, str]:
        """Return one named enum table."""

        try:
            return self.enum_tables[enum_key]
        except KeyError as exc:
            raise KeyError(enum_key) from exc

    def bit_labels_for(self, label_key: str) -> dict[int, str]:
        """Return one named bit-label table."""

        try:
            return self.bit_labels[label_key]
        except KeyError as exc:
            raise KeyError(label_key) from exc

    def scalar_register(self, register_key: str) -> int:
        """Return one named scalar register pointer."""

        try:
            return self.scalar_registers[register_key]
        except KeyError as exc:
            raise KeyError(register_key) from exc

    def measurement_description(self, key: str) -> MeasurementDescription:
        """Return one named measurement description."""

        for description in self.measurement_descriptions:
            if description.key == key:
                return description
        raise KeyError(key)

    def binary_sensor_description(self, key: str) -> BinarySensorDescription:
        """Return one named binary sensor description."""

        for description in self.binary_sensor_descriptions:
            if description.key == key:
                return description
        raise KeyError(key)
