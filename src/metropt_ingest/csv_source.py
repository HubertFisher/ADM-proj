"""Streaming validation for the MetroPT-3 CSV and its distributed ZIP archive."""

from __future__ import annotations

import csv
import math
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from io import TextIOWrapper
from pathlib import Path
from typing import TextIO
from zipfile import BadZipFile, ZipFile, ZipInfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from metropt_ingest.errors import ConfigurationError, DataValidationError
from metropt_ingest.models import (
    ANALOGUE_SENSOR_FIELDS,
    DIGITAL_SOURCE_FIELDS,
    REQUIRED_CSV_FIELDS,
    SOURCE_SEQUENCE_FIELD,
    SensorReading,
)


@dataclass(frozen=True, slots=True)
class HeaderLayout:
    width: int
    source_sequence: int
    timestamp: int
    analogue: Mapping[str, int]
    digital: Mapping[str, int]


class MetroPTCsvSource:
    """Stream the measured dataset from CSV or directly from its ZIP distribution."""

    def __init__(
        self,
        path: Path,
        *,
        source_timezone: str,
        archive_member: str | None = None,
        expected_source_step: int = 10,
        max_rows: int | None = None,
    ) -> None:
        if max_rows is not None and max_rows <= 0:
            raise ConfigurationError("max_rows must be greater than zero")
        if expected_source_step <= 0:
            raise ConfigurationError("expected_source_step must be greater than zero")
        try:
            timezone = ZoneInfo(source_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ConfigurationError(f"Unknown source timezone: {source_timezone}") from exc

        self._path = path
        self._source_timezone_name = source_timezone
        self._source_timezone = timezone
        self._archive_member = archive_member
        self._expected_source_step = expected_source_step
        self._max_rows = max_rows

    @property
    def source_timezone(self) -> str:
        return self._source_timezone_name

    def validate(self) -> None:
        """Fail before database connection if the input container or header is unusable."""
        with self._open_text() as stream:
            self._read_header(csv.reader(stream))

    def __iter__(self) -> Iterator[SensorReading]:
        try:
            with self._open_text() as stream:
                reader = csv.reader(stream)
                layout = self._read_header(reader)
                previous_source_sequence: int | None = None
                for row_number, row in enumerate(reader, start=2):
                    if self._max_rows is not None and row_number - 2 >= self._max_rows:
                        break
                    if len(row) != layout.width:
                        raise DataValidationError(
                            f"Invalid CSV row {row_number}: expected {layout.width} columns, "
                            f"found {len(row)}"
                        )
                    reading = self._parse_row(row, layout, row_number)
                    if previous_source_sequence is not None:
                        step = reading.source_sequence - previous_source_sequence
                        if step != self._expected_source_step:
                            raise DataValidationError(
                                f"Invalid CSV row {row_number}: source sequence step is {step}, "
                                f"expected {self._expected_source_step}"
                            )
                    previous_source_sequence = reading.source_sequence
                    yield reading
        except csv.Error as exc:
            raise DataValidationError(f"Malformed CSV data: {exc}") from exc

    @contextmanager
    def _open_text(self) -> Iterator[TextIO]:
        if not self._path.is_file():
            raise ConfigurationError(f"Input file does not exist: {self._path}")

        suffix = self._path.suffix.casefold()
        try:
            if suffix == ".csv":
                with self._path.open("r", encoding="utf-8-sig", newline="") as stream:
                    yield stream
                return
            if suffix != ".zip":
                raise ConfigurationError("Input must be a .csv file or a .zip containing one CSV")

            with ZipFile(self._path) as archive:
                member = self._resolve_archive_member(archive)
                if member.flag_bits & 0x1:
                    raise ConfigurationError(
                        f"Encrypted archive member is not supported: {member.filename}"
                    )
                with (
                    archive.open(member, "r") as binary_stream,
                    TextIOWrapper(binary_stream, encoding="utf-8-sig", newline="") as stream,
                ):
                    yield stream
        except BadZipFile as exc:
            raise ConfigurationError(f"Invalid ZIP archive {self._path}: {exc}") from exc
        except OSError as exc:
            raise ConfigurationError(f"Unable to read input file {self._path}: {exc}") from exc

    def _resolve_archive_member(self, archive: ZipFile) -> ZipInfo:
        if self._archive_member is not None:
            try:
                member = archive.getinfo(self._archive_member)
            except KeyError as exc:
                raise ConfigurationError(
                    f"CSV member not found in archive: {self._archive_member}"
                ) from exc
            if member.is_dir():
                raise ConfigurationError(f"Archive member is a directory: {member.filename}")
            return member

        candidates = [
            member
            for member in archive.infolist()
            if not member.is_dir() and member.filename.casefold().endswith(".csv")
        ]
        if len(candidates) != 1:
            raise ConfigurationError(
                "ZIP input must contain exactly one CSV unless --archive-member is provided; "
                f"found {len(candidates)}"
            )
        return candidates[0]

    @staticmethod
    def _read_header(reader: Iterator[list[str]]) -> HeaderLayout:
        try:
            fieldnames = next(reader)
        except StopIteration as exc:
            raise DataValidationError("CSV is empty or does not contain a header") from exc
        if len(fieldnames) != len(set(fieldnames)):
            raise DataValidationError("CSV header contains duplicate column names")

        missing = sorted(set(REQUIRED_CSV_FIELDS).difference(fieldnames))
        if missing:
            labels = [field if field else "<unnamed source sequence>" for field in missing]
            raise DataValidationError(f"CSV is missing required columns: {', '.join(labels)}")
        positions = {field: position for position, field in enumerate(fieldnames)}
        return HeaderLayout(
            width=len(fieldnames),
            source_sequence=positions[SOURCE_SEQUENCE_FIELD],
            timestamp=positions["timestamp"],
            analogue={field: positions[field] for field in ANALOGUE_SENSOR_FIELDS},
            digital={
                canonical: positions[source] for source, canonical in DIGITAL_SOURCE_FIELDS.items()
            },
        )

    def _parse_row(
        self, row: Sequence[str], layout: HeaderLayout, row_number: int
    ) -> SensorReading:
        try:
            source_sequence = self._parse_source_sequence(row[layout.source_sequence])
            timestamp = self._parse_timestamp(row[layout.timestamp])
            analogue = {
                field: self._parse_analogue(row[position], field)
                for field, position in layout.analogue.items()
            }
            digital = {
                field: self._parse_digital(row[position], field)
                for field, position in layout.digital.items()
            }
            return SensorReading(
                source_sequence=source_sequence,
                timestamp=timestamp,
                analogue=analogue,
                digital=digital,
            )
        except (IndexError, TypeError, ValueError) as exc:
            raise DataValidationError(f"Invalid CSV row {row_number}: {exc}") from exc

    @staticmethod
    def _parse_source_sequence(raw_value: str) -> int:
        value = int(raw_value)
        if value < 0:
            raise ValueError("source sequence must not be negative")
        return value

    def _parse_timestamp(self, raw_value: str) -> datetime:
        value = raw_value.strip()
        if (
            len(value) != 19
            or value[4] != "-"
            or value[7] != "-"
            or value[10] != " "
            or value[13] != ":"
            or value[16] != ":"
        ):
            raise ValueError("timestamp must match YYYY-MM-DD HH:MM:SS")
        local_timestamp = datetime.fromisoformat(value)
        if local_timestamp.tzinfo is not None:
            raise ValueError("source timestamp must not contain a timezone offset")
        return local_timestamp.replace(tzinfo=self._source_timezone).astimezone(UTC)

    @staticmethod
    def _parse_analogue(raw_value: str, field: str) -> float:
        value = float(raw_value)
        if not math.isfinite(value):
            raise ValueError(f"{field} must be a finite number")
        return value

    @staticmethod
    def _parse_digital(raw_value: str, field: str) -> int:
        value = float(raw_value)
        if not value.is_integer() or int(value) not in (0, 1):
            raise ValueError(f"{field} must be 0 or 1")
        return int(value)
