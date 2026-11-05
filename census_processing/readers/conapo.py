from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

CONAPO_ARCHIVE = "00_Republica_mexicana.zip"
CONAPO_WORKBOOK = "3_Indicadores_Dem_00_RM.xlsx"
CONAPO_COLUMNS = {"CLAVE": "cvegeo", "AÑO": "year", "POB_MIT_MUN": "pob_mit_mun"}


def read_conapo_archive(path: Path) -> pd.DataFrame:
    """Read municipality populations without writing files or database tables."""
    with ZipFile(path) as archive:
        members = [
            member
            for member in archive.infolist()
            if not member.is_dir() and Path(member.filename).name == CONAPO_WORKBOOK
        ]
        if len(members) != 1:
            msg = (
                f"Expected exactly one {CONAPO_WORKBOOK} in {path}; "
                f"found {len(members)}"
            )
            raise ValueError(msg)
        workbook_bytes = BytesIO(archive.read(members[0]))

    with pd.ExcelFile(workbook_bytes, engine="openpyxl") as workbook:
        if len(workbook.sheet_names) != 1:
            msg = f"Expected exactly one worksheet in {CONAPO_WORKBOOK}"
            raise ValueError(msg)
        frame = pd.read_excel(
            workbook,
            sheet_name=0,
            usecols=list(CONAPO_COLUMNS),
            dtype="string",
            keep_default_na=False,
        ).rename(columns=CONAPO_COLUMNS)

    if frame.empty:
        msg = "CONAPO workbook contains no population records"
        raise ValueError(msg)

    for column, lower, upper in (
        ("cvegeo", 1, 99999),
        ("year", -(2**31), 2**31 - 1),
        ("pob_mit_mun", 0, 2**31 - 1),
    ):
        values = pd.to_numeric(frame[column], errors="raise")
        valid = values.notna() & values.between(lower, upper) & values.mod(1).eq(0)
        if not valid.all():
            msg = (
                f"CONAPO {column} must contain non-null integers in [{lower}, {upper}]"
            )
            raise ValueError(msg)
        frame[column] = values.astype("int32")

    frame["cvegeo"] = frame["cvegeo"].astype("string").str.zfill(5)
    if frame.duplicated(["cvegeo", "year"]).any():
        msg = "CONAPO contains duplicate municipality/year pairs"
        raise ValueError(msg)

    return frame[list(CONAPO_COLUMNS.values())]
