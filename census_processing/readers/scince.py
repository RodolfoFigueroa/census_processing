import subprocess
import tempfile
import zipfile
from collections.abc import Generator, Iterable, Mapping
from contextlib import contextmanager
from itertools import islice
from pathlib import Path

import pandas as pd
from dbfread import DBF

SCINCE_TABLES = {
    2010: {
        "pob": "{geography}.dbf",
        "des": "tablas/cpv2010_{geography}_desarrollo_social.dbf",
        "disc": "tablas/cpv2010_{geography}_discapacidad.dbf",
        "eco": "tablas/cpv2010_{geography}_caracteristicas_economicas.dbf",
        "edu": "tablas/cpv2010_{geography}_caracteristicas_educativas.dbf",
        "fec": "tablas/cpv2010_{geography}_fecundidad.dbf",
        "hogar": "tablas/cpv2010_{geography}_hogares_censales.dbf",
        "indi": "tablas/cpv2010_{geography}_lengua_indigena.dbf",
        "mig": "tablas/cpv2010_{geography}_migracion.dbf",
        "mor": "tablas/cpv2010_{geography}_mortalidad.dbf",
        "relig": "tablas/cpv2010_{geography}_religion.dbf",
        "salud": "tablas/cpv2010_{geography}_servicios_de_salud.dbf",
        "scony": "tablas/cpv2010_{geography}_situacion_conyugal.dbf",
        "viv": "tablas/cpv2010_{geography}_viviendas.dbf",
    },
    2020: {
        "pob": "cartografia/{geography}.dbf",
        "disc": "tablas/cpv2020_{geography}_discapacidad.dbf",
        "eco": "tablas/cpv2020_{geography}_caracteristicas_economicas.dbf",
        "edu": "tablas/cpv2020_{geography}_educacion.dbf",
        "fec": "tablas/cpv2020_{geography}_fecundidad.dbf",
        "hogar": "tablas/cpv2020_{geography}_hogares_censales.dbf",
        "indi": "tablas/cpv2020_{geography}_etnicidad.dbf",
        "mig": "tablas/cpv2020_{geography}_migracion.dbf",
        "mor": "tablas/cpv2020_{geography}_mortalidad.dbf",
        "relig": "tablas/cpv2020_{geography}_religion.dbf",
        "salud": "tablas/cpv2020_{geography}_servicios_de_salud.dbf",
        "scony": "tablas/cpv2020_{geography}_situacion_conyugal.dbf",
        "viv": "tablas/cpv2020_{geography}_vivienda.dbf",
    },
}
SCINCE_LEVELS = ("ent", "mun", "loc", "ageb", "mza")
STRING_DTYPE = pd.StringDtype(storage="pyarrow")


def scince_members(year: int, level: str) -> dict[str, tuple[str, ...]]:
    """Map the fixed source layout without specifying any indicator columns."""
    geographies = {
        "ent": ("estatal",),
        "mun": ("municipal",),
        "loc": ("loc_urb", "loc_rur"),
        "ageb": ("ageb_urb",),
        "mza": ("manzanas" if year == 2010 else "manzana",),
    }[level]
    return {
        theme: tuple(template.format(geography=geo) for geo in geographies)
        for theme, template in SCINCE_TABLES[year].items()
        if not (theme == "des" and level == "mza")
    }


def read_scince_dbf(path: Path) -> pd.DataFrame:
    """Decode all published attributes without numeric inference."""
    table = DBF(path, raw=True, encoding="latin-1", ignorecase=False)
    # OID is a shapefile row identifier, absent from some states' DBFs.
    fields = [field for field in table.fields if field.name.lower() != "oid"]
    names = [field.name.lower() for field in fields]
    frames = []
    records = iter(table)
    while batch := list(islice(records, 10_000)):
        values = {
            field.name.lower(): [
                (
                    record[field.name].rstrip(b" \0")
                    if field.type == "C"
                    else record[field.name].strip(b" \0")
                ).decode("latin-1")
                for record in batch
            ]
            for field in fields
        }
        frames.append(pd.DataFrame(values, dtype=STRING_DTYPE))
    return (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=names, dtype=STRING_DTYPE)
    )


@contextmanager
def extracted_scince_archive(
    directory: Path, year: int, state_code: str
) -> Generator[tuple[Path, Path, str]]:
    """Extract once for all themes, cleaning up even if processing fails."""
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
            root = extraction_root / state_code
        else:
            installer = extraction_root / f"SCINCE2020_DATOS_{state_code}.exe"
            extraction_root = Path(temporary) / "installer"
            subprocess.run(  # noqa: S603 -- fixed executable; paths are separate arguments.
                ["/usr/bin/innoextract", str(installer), "-d", str(extraction_root)],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            root = next((extraction_root / "app").glob(f"{state_code}*"))
        yield root, extraction_root, filename


def read_scince_theme(
    members: Mapping[str, Path],
    extraction_root: Path,
    relatives: tuple[str, ...],
    theme: str,
) -> pd.DataFrame:
    frames = []
    for relative in relatives:
        path = members[relative.casefold()]
        frame = read_scince_dbf(path)
        frame[f"_source_member_{theme}"] = pd.Series(
            path.relative_to(extraction_root).as_posix(),
            index=frame.index,
            dtype=STRING_DTYPE,
        )
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def combine_scince_themes(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    remaining = iter(frames)
    combined = next(remaining)
    for frame in remaining:
        combined = combined.merge(
            frame,
            on="cvegeo",
            how="outer",
            validate="one_to_one",
            suffixes=(None, None),
        )
    return combined


def read_scince_level(
    root: Path,
    extraction_root: Path,
    year: int,
    level: str,
) -> pd.DataFrame:
    members = {
        path.relative_to(root).as_posix().casefold(): path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() == ".dbf"
    }
    return combine_scince_themes(
        read_scince_theme(members, extraction_root, relatives, theme)
        for theme, relatives in scince_members(year, level).items()
    )


def read_scince_archive(
    directory: Path, year: int, state_code: str, level: str
) -> pd.DataFrame:
    with extracted_scince_archive(directory, year, state_code) as (
        root,
        extraction_root,
        filename,
    ):
        frame = read_scince_level(root, extraction_root, year, level)
        frame["_source_file"] = pd.Series(
            filename, index=frame.index, dtype=STRING_DTYPE
        )
        return frame


def load_scince(directory: Path, year: int, level: str) -> pd.DataFrame:
    """Keep only combined Arrow-backed state frames until the national concatenation."""
    return pd.concat(
        [
            read_scince_archive(directory, year, f"{state:02d}", level)
            for state in range(1, 33)
        ],
        ignore_index=True,
    )
