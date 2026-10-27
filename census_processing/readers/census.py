import csv
import io
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

import pandas as pd


@dataclass(frozen=True)
class CensusSource:
    archive_template: str
    member_template: str


CENSUS_SOURCES = {
    2010: CensusSource(
        archive_template="resageburb_{state:02d}_2010_csv.zip",
        member_template=(
            "resultados_ageb_urbana_{state:02d}_cpv2010/conjunto_de_datos/"
            "resultados_ageb_urbana_{state:02d}_cpv2010.csv"
        ),
    ),
    2020: CensusSource(
        archive_template="ageb_mza_urbana_{state:02d}_cpv2020_csv.zip",
        member_template=(
            "ageb_mza_urbana_{state:02d}_cpv2020/conjunto_de_datos/"
            "conjunto_de_datos_ageb_urbana_{state:02d}_cpv2020.csv"
        ),
    ),
}

IDENTIFIER_PATTERNS = {
    "entidad": r"[0-9]{1,2}",
    "mun": r"[0-9]{1,3}",
    "loc": r"[0-9]{1,4}",
    "ageb": r"[0-9A-Za-z]{1,4}",
    "mza": r"[0-9]{1,3}",
}
REQUIRED_COLUMNS = {*IDENTIFIER_PATTERNS, "nom_ent", "nom_mun", "nom_loc"}
HEADER_REPAIRS = {"ï»¿ENTIDAD": "ENTIDAD", 'ï»¿"entidad"': "entidad"}


def _read_member(archive: ZipFile, member: str, encoding: str) -> pd.DataFrame:
    # Inspect the original header before pandas can rename duplicate columns.
    with (
        archive.open(member) as source,
        io.TextIOWrapper(source, encoding=encoding, newline="") as text,
    ):
        header = next(csv.reader(text), [])
    columns = [HEADER_REPAIRS.get(name, name).lower() for name in header]
    if len(set(columns)) != len(columns):
        msg = f"Duplicate normalized columns in {member}"
        raise ValueError(msg)
    missing = REQUIRED_COLUMNS - set(columns)
    if missing:
        msg = f"Missing required columns in {member}: {sorted(missing)}"
        raise ValueError(msg)
    if "_source_file" in columns:
        msg = f"Reserved provenance column _source_file in {member}"
        raise ValueError(msg)

    with archive.open(member) as source:
        return pd.read_csv(
            source,
            encoding=encoding,
            header=0,
            names=columns,
            dtype="string",
            keep_default_na=False,
        )


def read_census_archive(directory: Path, year: int, state: int) -> pd.DataFrame:
    """Decode one state archive without interpreting its statistical values."""
    if year not in CENSUS_SOURCES:
        msg = f"Unsupported census year: {year}"
        raise ValueError(msg)
    if not 1 <= state <= 32:
        msg = f"Invalid state code: {state}"
        raise ValueError(msg)
    specification = CENSUS_SOURCES[year]
    path = directory / specification.archive_template.format(state=state)
    member = specification.member_template.format(state=state)
    with ZipFile(path) as archive:
        try:
            frame = _read_member(archive, member, "utf-8-sig")
        except UnicodeDecodeError:
            frame = _read_member(archive, member, "latin1")

    for column, pattern in IDENTIFIER_PATTERNS.items():
        if not frame[column].str.fullmatch(pattern).all():
            msg = f"Invalid {column} identifiers in {path.name}"
            raise ValueError(msg)
    return frame.assign(_source_file=path.name)


def load_census(directory: Path, year: int) -> pd.DataFrame:
    """Read all 32 states, failing before publication if their headers differ."""
    frames: list[pd.DataFrame] = []
    expected_columns: list[str] | None = None
    for state in range(1, 33):
        frame = read_census_archive(directory, year, state)
        columns = frame.columns.tolist()
        if expected_columns is not None and columns != expected_columns:
            msg = f"Inconsistent census columns for {year}, state {state:02d}"
            raise ValueError(msg)
        expected_columns = columns
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)
