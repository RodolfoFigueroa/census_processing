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


@pytest.mark.parametrize("year", [2010, 2020])
@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "latin1"])
def test_reader_preserves_source_values(
    tmp_path: Path, year: int, encoding: str
) -> None:
    markers = ("*", "N/D", "", "0", "NA", "NULL", "1.0")
    rows = [(*ROW[:8], marker, ROW[9]) for marker in markers]
    filename = write_archive(tmp_path, year, csv_bytes(rows=rows, encoding=encoding))

    result = read_census_archive(tmp_path, year, 1)

    assert result.columns.tolist() == [name.lower() for name in HEADER] + [
        "_source_file"
    ]
    assert result["pobtot"].tolist() == list(markers)
    assert result["graproes"].tolist() == ["8.50"] * len(markers)
    assert result["nom_ent"].tolist() == ["México"] * len(markers)
    for column, value in {
        "entidad": "01",
        "mun": "001",
        "loc": "0001",
        "ageb": "001A",
        "mza": "000",
    }.items():
        assert result[column].tolist() == [value] * len(markers)
    assert result["_source_file"].tolist() == [filename] * len(markers)
    assert not result.isna().any().any()


@pytest.mark.parametrize("first_column", ["ï»¿ENTIDAD", 'ï»¿"entidad"'])
def test_reader_repairs_legacy_headers(tmp_path: Path, first_column: str) -> None:
    data = csv_bytes(header=(first_column, *HEADER[1:]), encoding="latin1")
    write_archive(tmp_path, 2010, data)
    assert read_census_archive(tmp_path, 2010, 1)["entidad"].tolist() == ["01"]


@pytest.mark.parametrize("duplicate", ["ENTIDAD", "entidad"])
def test_duplicate_headers_are_rejected_before_pandas_mangles_them(
    tmp_path: Path,
    duplicate: str,
) -> None:
    write_archive(tmp_path, 2020, csv_bytes((*HEADER, duplicate), [(*ROW, "01")]))
    with pytest.raises(ValueError, match="Duplicate normalized columns"):
        read_census_archive(tmp_path, 2020, 1)


@pytest.mark.parametrize(
    "column", ["ENTIDAD", "NOM_ENT", "MUN", "NOM_MUN", "LOC", "NOM_LOC", "AGEB", "MZA"]
)
def test_required_source_columns(tmp_path: Path, column: str) -> None:
    pairs = [
        (name, value) for name, value in zip(HEADER, ROW, strict=True) if name != column
    ]
    write_archive(
        tmp_path, 2020, csv_bytes([p[0] for p in pairs], [[p[1] for p in pairs]])
    )
    with pytest.raises(ValueError, match="Missing required columns"):
        read_census_archive(tmp_path, 2020, 1)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("ENTIDAD", "001"),
        ("MUN", "0001"),
        ("LOC", "00001"),
        ("AGEB", "00001"),
        ("MZA", "0001"),
        ("ENTIDAD", ""),
        ("MUN", "1.0"),
        ("LOC", "NA"),
        ("AGEB", "00-A"),
        ("MZA", " 1"),
    ],
)
def test_invalid_identifiers_fail_before_padding(
    tmp_path: Path, column: str, value: str
) -> None:
    row = list(ROW)
    row[HEADER.index(column)] = value
    write_archive(tmp_path, 2020, csv_bytes(rows=[row]))
    with pytest.raises(ValueError, match=f"Invalid {column.lower()} identifiers"):
        read_census_archive(tmp_path, 2020, 1)


def test_provenance_column_cannot_be_overwritten(tmp_path: Path) -> None:
    write_archive(
        tmp_path, 2020, csv_bytes((*HEADER, "_source_file"), [(*ROW, "fake")])
    )
    with pytest.raises(ValueError, match="Reserved provenance column"):
        read_census_archive(tmp_path, 2020, 1)


def test_missing_archive_and_member_fail(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_census_archive(tmp_path, 2020, 1)
    filename, _ = archive_names(2020, 1)
    with ZipFile(tmp_path / filename, "w") as archive:
        archive.writestr("wrong.csv", csv_bytes())
    with pytest.raises(KeyError, match="There is no item named"):
        read_census_archive(tmp_path, 2020, 1)


@pytest.mark.parametrize(("year", "state"), [(2000, 1), (2020, 0), (2020, 33)])
def test_unsupported_source_selection(tmp_path: Path, year: int, state: int) -> None:
    with pytest.raises(ValueError, match=r"Unsupported census year|Invalid state code"):
        read_census_archive(tmp_path, year, state)


@pytest.mark.parametrize("year", [2010, 2020])
def test_loads_every_state_and_preserves_repeated_records(
    tmp_path: Path, year: int
) -> None:
    filenames = [
        write_archive(tmp_path, year, csv_bytes(rows=[ROW, ROW]), state)
        for state in range(1, 33)
    ]
    result = load_census(tmp_path, year)
    assert len(result) == 64
    assert result.index.tolist() == list(range(64))
    assert result["_source_file"].tolist() == [
        filename for filename in filenames for _ in range(2)
    ]
    assert result["ageb"].tolist() == ["001A"] * 64
    assert all(
        isinstance(result[column].dtype, pd.StringDtype)
        for column in result.columns
        if column != "_source_file"
    )


def test_inconsistent_state_headers_fail(tmp_path: Path) -> None:
    write_archive(tmp_path, 2020, csv_bytes(), 1)
    write_archive(tmp_path, 2020, csv_bytes(HEADER[:-1], [ROW[:-1]]), 2)
    with pytest.raises(
        ValueError, match="Inconsistent census columns for 2020, state 02"
    ):
        load_census(tmp_path, 2020)
