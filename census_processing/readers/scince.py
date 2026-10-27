import re
import subprocess
import tempfile
import zipfile
from collections.abc import Iterable, Sequence
from pathlib import Path

import pandas as pd
from dbfread import DBF

SCINCE_TABLES = {
    2010: {
        "pob": "ageb_urb.dbf",
        **{
            theme: f"tablas/cpv2010_ageb_urb_{label}.dbf"
            for theme, label in {
                "des": "desarrollo_social",
                "disc": "discapacidad",
                "eco": "caracteristicas_economicas",
                "edu": "caracteristicas_educativas",
                "fec": "fecundidad",
                "hogar": "hogares_censales",
                "indi": "lengua_indigena",
                "mig": "migracion",
                "mor": "mortalidad",
                "relig": "religion",
                "salud": "servicios_de_salud",
                "scony": "situacion_conyugal",
                "viv": "viviendas",
            }.items()
        },
    },
    2020: {
        "pob": "cartografia/ageb_urb.dbf",
        **{
            theme: f"tablas/cpv2020_ageb_urb_{label}.dbf"
            for theme, label in {
                "disc": "discapacidad",
                "eco": "caracteristicas_economicas",
                "edu": "educacion",
                "fec": "fecundidad",
                "hogar": "hogares_censales",
                "indi": "etnicidad",
                "mig": "migracion",
                "mor": "mortalidad",
                "relig": "religion",
                "salud": "servicios_de_salud",
                "scony": "situacion_conyugal",
                "viv": "vivienda",
            }.items()
        },
    },
}
PROVENANCE_COLUMNS = ("_source_file", "_source_member")


def _unique_path(paths: Iterable[Path], label: str) -> Path:
    matches = list(paths)
    if len(matches) != 1:
        msg = f"Expected exactly one {label}; found {len(matches)}: {matches}"
        raise ValueError(msg)
    return matches[0]


def _resolve_member(root: Path, relative: str) -> Path:
    current = root
    for part in Path(relative).parts:
        current = _unique_path(
            (p for p in current.iterdir() if p.name.casefold() == part.casefold()),
            f"member {relative} in {root}",
        )
    if not current.is_file():
        msg = f"Expected a DBF file: {current}"
        raise ValueError(msg)
    return current


def read_scince_dbf(
    path: Path, *, source_file: str, source_member: str
) -> pd.DataFrame:
    """Decode DBF storage without numeric inference or indicator selection."""
    table = DBF(path, raw=True, encoding="latin-1", ignorecase=False)
    names = [field.name.lower() for field in table.fields]
    if len(names) != len(set(names)):
        msg = f"Duplicate normalized columns in {path}: {names}"
        raise ValueError(msg)
    if "cvegeo" not in names or set(names).intersection(PROVENANCE_COLUMNS):
        msg = f"Missing cvegeo or reserved provenance columns in {path}: {names}"
        raise ValueError(msg)

    # OID is a shapefile row identifier, absent from some states' DBFs.
    fields = [field for field in table.fields if field.name.lower() != "oid"]
    for field in fields:
        if field.type not in {"C", "N", "F"}:
            msg = f"Unsupported DBF type {field.type} for {path}:{field.name}"
            raise ValueError(msg)

    records = []
    for record in table:
        values = []
        for field in fields:
            raw = record[field.name]
            padded = raw.rstrip(b" \0") if field.type == "C" else raw.strip(b" \0")
            values.append(padded.decode("latin-1"))
        records.append(values)
    frame = pd.DataFrame(
        records, columns=[field.name.lower() for field in fields], dtype="string"
    )
    invalid = ~frame["cvegeo"].str.fullmatch(r"[0-9]{9}[0-9A-Za-z]{4}", na=False)
    if invalid.any():
        msg = (
            f"Invalid cvegeo in {path}: {frame.loc[invalid, 'cvegeo'].head().tolist()}"
        )
        raise ValueError(msg)
    for name, value in zip(
        PROVENANCE_COLUMNS, (source_file, source_member), strict=True
    ):
        frame[name] = pd.Series(value, index=frame.index, dtype="string")
    return frame


def _read_tables(
    root: Path, extraction_root: Path, year: int, source_file: str
) -> dict[str, pd.DataFrame]:
    frames = {}
    for theme, relative in SCINCE_TABLES[year].items():
        path = _resolve_member(root, relative)
        frames[theme] = read_scince_dbf(
            path,
            source_file=source_file,
            source_member=path.relative_to(extraction_root).as_posix(),
        )
    return frames


def read_scince_archive(
    directory: Path, year: int, state_code: str
) -> dict[str, pd.DataFrame]:
    """Read all themes from one state's archive, sharing extraction work."""
    if year not in SCINCE_TABLES:
        msg = f"Unsupported SCINCE year: {year}"
        raise ValueError(msg)
    if not re.fullmatch(r"0[1-9]|[12][0-9]|3[0-2]", state_code):
        msg = f"Invalid state code: {state_code}"
        raise ValueError(msg)
    filename = (
        f"{state_code}_SCINCE_zip.zip"
        if year == 2010
        else f"SCINCE2020_DATOS_{state_code}.zip"
    )
    with (
        zipfile.ZipFile(directory / filename) as archive,
        tempfile.TemporaryDirectory() as temporary,
    ):
        extraction_root = Path(temporary) / "archive"
        archive.extractall(extraction_root)
        if year == 2010:
            root = _unique_path(
                (p for p in extraction_root.iterdir() if p.name == state_code),
                f"state directory {state_code}",
            )
        else:
            installer = _unique_path(
                (
                    p
                    for p in extraction_root.rglob("*")
                    if p.is_file() and p.suffix.lower() == ".exe"
                ),
                "SCINCE installer",
            )
            extraction_root = Path(temporary) / "installer"
            subprocess.run(  # noqa: S603 -- fixed executable; paths are separate arguments.
                ["/usr/bin/innoextract", str(installer), "-d", str(extraction_root)],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            root = _unique_path(
                (
                    p
                    for p in (extraction_root / "app").glob(f"{state_code}*")
                    if p.is_dir()
                ),
                f"state directory {state_code}",
            )
        return _read_tables(root, extraction_root, year, filename)


def concatenate_scince_theme(
    frame_maps: Sequence[dict[str, pd.DataFrame]], theme: str
) -> pd.DataFrame:
    """Align state columns without silently unioning incompatible schemas."""
    if not frame_maps:
        msg = f"No state frames supplied for SCINCE theme {theme}"
        raise ValueError(msg)
    columns = frame_maps[0][theme].columns.tolist()
    frames = []
    for state, frame_map in enumerate(frame_maps, start=1):
        frame = frame_map[theme]
        if not frame.columns.is_unique or set(frame.columns) != set(columns):
            msg = f"SCINCE schema mismatch for state {state:02d}, theme {theme}"
            raise ValueError(msg)
        frames.append(frame.loc[:, columns])
    return pd.concat(frames, ignore_index=True)
