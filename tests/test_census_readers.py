import csv
import io
from collections.abc import Sequence
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest

from census_processing.readers.census import load_census, read_census_archive

HEADER = (
    "ENTIDAD",
    "NOM_ENT",
    "MUN",
    "NOM_MUN",
    "LOC",
    "NOM_LOC",
    "AGEB",
    "MZA",
    "POBTOT",
    "GRAPROES",
)
ROW = (
    "01",
    "México",
    "001",
    "Municipio",
    "0001",
    "Localidad",
    "001A",
    "000",
    "*",
    "8.50",
)


def archive_names(year: int, state: int) -> tuple[str, str]:
    if year == 2010:
        return (
            f"resageburb_{state:02d}_2010_csv.zip",
            (
                f"resultados_ageb_urbana_{state:02d}_cpv2010/conjunto_de_datos/"
                f"resultados_ageb_urbana_{state:02d}_cpv2010.csv"
            ),
        )
    return (
        f"ageb_mza_urbana_{state:02d}_cpv2020_csv.zip",
        (
            f"ageb_mza_urbana_{state:02d}_cpv2020/conjunto_de_datos/"
            f"conjunto_de_datos_ageb_urbana_{state:02d}_cpv2020.csv"
        ),
    )


def csv_bytes(
    header: Sequence[str] = HEADER,
    rows: Sequence[Sequence[str]] = (ROW,),
    encoding: str = "utf-8",
) -> bytes:
    buffer = io.StringIO(newline="")
    csv.writer(buffer).writerows([header, *rows])
    return buffer.getvalue().encode(encoding)


def write_archive(directory: Path, year: int, data: bytes, state: int = 1) -> str:
    filename, member = archive_names(year, state)
    with ZipFile(directory / filename, "w") as archive:
        archive.writestr("conjunto_de_datos/dictionary.csv", "not the census data")
        archive.writestr(member, data)
    return filename


@pytest.mark.parametrize(
    ("year", "state", "encoding"),
    [(2010, 1, "utf-8-sig"), (2020, 1, "utf-8"), (2020, 14, "latin1")],
)
def test_reader_preserves_source_values(
    tmp_path: Path, year: int, state: int, encoding: str
) -> None:
    markers = ("*", "N/D", "N/A", "0", "42")
    rows = [(f"{state:02d}", *ROW[1:8], marker, ROW[9]) for marker in markers]
    filename = write_archive(
        tmp_path, year, csv_bytes(rows=rows, encoding=encoding), state
    )

    result = read_census_archive(tmp_path, year, state)

    assert result.columns.tolist() == [name.lower() for name in HEADER] + [
        "_source_file"
    ]
    assert result["pobtot"].tolist() == list(markers)
    assert result["graproes"].tolist() == ["8.50"] * len(markers)
    assert result["nom_ent"].tolist() == ["México"] * len(markers)
    for column, value in {
        "entidad": f"{state:02d}",
        "mun": "001",
        "loc": "0001",
        "ageb": "001A",
        "mza": "000",
    }.items():
        assert result[column].tolist() == [value] * len(markers)
    assert result["_source_file"].tolist() == [filename] * len(markers)
    assert not result.isna().any().any()


@pytest.mark.parametrize("year", [2010, 2020])
def test_loads_every_state_and_preserves_repeated_records(
    tmp_path: Path, year: int
) -> None:
    filenames = [
        write_archive(
            tmp_path,
            year,
            csv_bytes(
                rows=[ROW, ROW],
                encoding="latin1" if (year, state) == (2020, 14) else "utf-8",
            ),
            state,
        )
        for state in range(1, 33)
    ]
    result = load_census(tmp_path, year)
    assert len(result) == 64
    assert result.index.tolist() == list(range(64))
    assert result["_source_file"].tolist() == [
        filename for filename in filenames for _ in range(2)
    ]
    assert result["ageb"].tolist() == ["001A"] * 64
    assert result["nom_ent"].tolist() == ["México"] * 64
    assert all(
        isinstance(result[column].dtype, pd.StringDtype)
        for column in result.columns
        if column != "_source_file"
    )
