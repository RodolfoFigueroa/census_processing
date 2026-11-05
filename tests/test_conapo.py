from collections.abc import Sequence
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest
from openpyxl import Workbook

from census_processing.readers.conapo import (
    CONAPO_ARCHIVE,
    CONAPO_WORKBOOK,
    read_conapo_archive,
)

HEADER = ("CLAVE", "NOM_MUN", "AÑO", "POB_MIT_MUN")


def write_archive(
    directory: Path,
    rows: Sequence[Sequence[int | float | str | None]],
    *,
    header: Sequence[str] = HEADER,
    members: Sequence[str] = (f"00_Republica_mexicana/{CONAPO_WORKBOOK}",),
    extra_sheet: bool = False,
) -> Path:
    workbook = Workbook()
    sheet = workbook.active
    assert sheet is not None
    sheet.append(list(header))
    for row in rows:
        sheet.append(list(row))
    if extra_sheet:
        workbook.create_sheet("Unexpected")
    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    path = directory / CONAPO_ARCHIVE
    with ZipFile(path, "w") as archive:
        archive.writestr("00_Republica_mexicana/1_Grupo_Quinq_00_RM.xlsx", b"ignored")
        for member in members:
            archive.writestr(member, buffer.getvalue())
    return path


def test_reader_preserves_population_and_all_years(tmp_path: Path) -> None:
    path = write_archive(
        tmp_path,
        [
            (1001, "Aguascalientes", 1990, 499839),
            (1001.0, "Aguascalientes", 2040, 0),
            ("01002", "Asientos", "2020", "50000"),
            (32001, "Apozol", 2000, 6000),
        ],
    )
    expected = pd.DataFrame(
        {
            "cvegeo": pd.Series(["01001", "01001", "01002", "32001"], dtype="string"),
            "year": pd.Series([1990, 2040, 2020, 2000], dtype="int32"),
            "pob_mit_mun": pd.Series([499839, 0, 50000, 6000], dtype="int32"),
        }
    )
    pd.testing.assert_frame_equal(read_conapo_archive(path), expected)


@pytest.mark.parametrize(
    "members",
    [(), (f"one/{CONAPO_WORKBOOK}", f"two/{CONAPO_WORKBOOK}")],
)
def test_reader_requires_one_matching_workbook(
    tmp_path: Path, members: tuple[str, ...]
) -> None:
    path = write_archive(tmp_path, [], members=members)
    with pytest.raises(ValueError, match=r"Expected exactly one.*xlsx"):
        read_conapo_archive(path)


def test_reader_requires_one_sheet(tmp_path: Path) -> None:
    path = write_archive(tmp_path, [(1001, "A", 1990, 10)], extra_sheet=True)
    with pytest.raises(ValueError, match="exactly one worksheet"):
        read_conapo_archive(path)


@pytest.mark.parametrize("missing", ["CLAVE", "AÑO", "POB_MIT_MUN"])
def test_reader_requires_columns(tmp_path: Path, missing: str) -> None:
    path = write_archive(tmp_path, [], header=[c for c in HEADER if c != missing])
    with pytest.raises(ValueError, match="Usecols do not match"):
        read_conapo_archive(path)


@pytest.mark.parametrize(
    ("index", "value"),
    [
        (0, None),
        (0, "invalid"),
        (0, 1001.5),
        (0, 100001),
        (0, -1001),
        (0, 0),
        (2, None),
        (2, 1990.5),
        (2, 2**31),
        (3, None),
        (3, "N/D"),
        (3, 42.5),
        (3, -1),
        (3, 2**31),
    ],
)
def test_reader_rejects_invalid_values(
    tmp_path: Path, index: int, value: float | str | None
) -> None:
    row: list[int | float | str | None] = [1001, "A", 1990, 10]
    row[index] = value
    path = write_archive(tmp_path, [row])
    with pytest.raises(ValueError, match=r"CONAPO|Unable to parse string"):
        read_conapo_archive(path)


def test_reader_rejects_normalized_duplicate_keys(tmp_path: Path) -> None:
    path = write_archive(tmp_path, [(1001, "A", 1990, 10), ("01001", "A", 1990, 20)])
    with pytest.raises(ValueError, match="duplicate municipality/year"):
        read_conapo_archive(path)


def test_reader_rejects_empty_data(tmp_path: Path) -> None:
    path = write_archive(tmp_path, [])
    with pytest.raises(ValueError, match="no population records"):
        read_conapo_archive(path)
