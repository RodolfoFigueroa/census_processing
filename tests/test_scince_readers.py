import struct
import subprocess
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import Mock
from zipfile import ZipFile

import pandas as pd
import pytest

from census_processing.readers import scince

IDENTIFIER = "010010001001A"
FIELDS = (("CVEGEO", "C", 13), ("POB1", "N", 24), ("POB1_R", "N", 24))


def dbf_bytes(
    fields: Sequence[tuple[str, str, int]] = FIELDS,
    rows: Sequence[Sequence[str]] = ((IDENTIFIER, "42", "12.34000000"),),
) -> bytes:
    """Build small dBASE III fixtures without a writer dependency."""
    header = bytearray(32)
    header[0] = 3
    struct.pack_into(
        "<IHH",
        header,
        4,
        len(rows),
        33 + 32 * len(fields),
        1 + sum(f[2] for f in fields),
    )
    descriptors = bytearray()
    for name, kind, width in fields:
        field = bytearray(32)
        encoded = name.encode("ascii")
        field[: len(encoded)] = encoded
        field[11] = ord(kind)
        field[16] = width
        descriptors.extend(field)
    records = bytearray()
    for row in rows:
        records.extend(b" ")
        for (_, kind, width), value in zip(fields, row, strict=True):
            encoded = value.encode("latin-1")
            assert len(encoded) <= width
            records.extend(
                encoded.ljust(width) if kind == "C" else encoded.rjust(width)
            )
    return bytes(header + descriptors + b"\r" + records + b"\x1a")


def read_fixture(tmp_path: Path, data: bytes) -> pd.DataFrame:
    path = tmp_path / "attributes.dbf"
    path.write_bytes(data)
    return scince.read_scince_dbf(path)


@pytest.mark.parametrize("value", ["", "0", "*", "****", "N/D", "12,5", "* 12.50 *"])
def test_raw_values_remain_text(tmp_path: Path, value: str) -> None:
    frame = read_fixture(tmp_path, dbf_bytes(rows=[(IDENTIFIER, value, "12.34000000")]))
    assert frame.iloc[0].tolist() == [
        IDENTIFIER,
        value,
        "12.34000000",
    ]
    assert all(isinstance(dtype, pd.StringDtype) for dtype in frame.dtypes)


def test_character_fields_and_optional_oid(tmp_path: Path) -> None:
    fields = (("CVEGEO", "C", 13), ("GRADO_MARG", "C", 20), ("OID", "N", 4))
    frame = read_fixture(
        tmp_path, dbf_bytes(fields, [(IDENTIFIER, "  Muy bajo á", "1")])
    )
    assert frame.columns.tolist() == [
        "cvegeo",
        "grado_marg",
    ]
    assert frame.loc[0, "grado_marg"] == "  Muy bajo á"


def test_empty_table_preserves_headers_and_types(tmp_path: Path) -> None:
    frame = read_fixture(tmp_path, dbf_bytes(rows=[]))
    assert frame.empty
    assert frame.columns.tolist() == [
        "cvegeo",
        "pob1",
        "pob1_r",
    ]
    assert all(isinstance(dtype, pd.StringDtype) for dtype in frame.dtypes)


IDS = {
    "ent": ("01", "01"),
    "mun": ("01001", "01002"),
    "loc": ("010010001", "010010002"),
    "ageb": ("010010001001A", "010010001002B"),
    "mza": ("010010001001A001", "010010001002B002"),
}


def source_fixture(level: str, theme: str, member: str) -> bytes:
    fields = [("CVEGEO", "C", 24)]
    if theme == "pob":
        fields.extend([("POB1", "N", 24), ("POB1_R", "N", 24)])
        if level == "loc":
            metadata = "CVE_AGEB" if "loc_rur" in member else "CABECERA"
            fields.append((metadata, "C", 24))
    elif theme == "des":
        fields.append(("GRADO_MARG", "C", 24))
    else:
        name = "MOR1_R" if theme == "mor" else f"{theme.upper()}1"
        fields.append((name, "N", 24))
    first, second = IDS[level]
    key = second if theme == "mor" else first
    if "loc_rur" in member:
        key = "010010003"
    rows = [] if theme == "des" else [[key, *["42"] * (len(fields) - 1)]]
    return dbf_bytes(fields, rows)


def write_archive(directory: Path, year: int, level: str) -> None:
    name = "01_SCINCE_zip.zip" if year == 2010 else "SCINCE2020_DATOS_01.zip"
    with ZipFile(directory / name, "w") as archive:
        if year == 2010:
            for theme, members in scince.scince_members(year, level).items():
                for member in members:
                    archive.writestr(
                        "01/" + member.upper(),
                        source_fixture(level, theme, member),
                    )
        else:
            archive.writestr("SCINCE2020_DATOS_01.exe", b"installer fixture")


@pytest.mark.parametrize("year", [2010, 2020])
@pytest.mark.parametrize("level", list(IDS))
def test_archive_combines_all_published_rows_with_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, year: int, level: str
) -> None:
    write_archive(tmp_path, year, level)
    extraction_roots = []

    def extract(args: list[str], *, check: bool, stdout: int) -> None:
        assert args[0] == "/usr/bin/innoextract"
        assert Path(args[1]).read_bytes() == b"installer fixture"
        assert args[2] == "-d"
        assert check
        assert stdout == subprocess.DEVNULL
        root = Path(args[3])
        extraction_roots.append(root)
        for theme, members in scince.scince_members(year, level).items():
            for member in members:
                path = root / "app/01_Aguascalientes" / member.upper()
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(source_fixture(level, theme, member))

    extractor = Mock(side_effect=extract)
    monkeypatch.setattr(scince.subprocess, "run", extractor)
    frame = scince.read_scince_archive(tmp_path, year, "01", level).set_index("cvegeo")
    expected = set(IDS[level]) | ({"010010003"} if level == "loc" else set())
    assert set(frame.index) == expected
    assert frame.loc[IDS[level][0], "pob1"] == "42"
    assert frame.loc[IDS[level][0], "pob1_r"] == "42"
    if level == "loc":
        assert frame.loc[IDS[level][0], "cabecera"] == "42"
        assert pd.isna(frame.loc[IDS[level][0], "cve_ageb"])
        assert frame.loc["010010003", "cve_ageb"] == "42"
        assert pd.isna(frame.loc["010010003", "cabecera"])
    assert frame.loc[IDS[level][1], "mor1_r"] == "42"
    if level != "ent":
        assert pd.isna(frame.loc[IDS[level][1], "pob1"])
        assert pd.isna(frame.loc[IDS[level][1], "_source_member_pob"])
        assert pd.isna(frame.loc[IDS[level][0], "mor1_r"])
    if year == 2010 and level != "mza":
        assert frame["grado_marg"].isna().all()
        assert frame["_source_member_des"].isna().all()
    else:
        assert "grado_marg" not in frame
        assert "_source_member_des" not in frame
    prefix = "01/" if year == 2010 else "app/01_Aguascalientes/"
    member = scince.scince_members(year, level)["pob"][0]
    assert frame.loc[IDS[level][0], "_source_member_pob"] == prefix + member.upper()
    assert frame["_source_file"].eq(frame["_source_file"].iloc[0]).all()
    assert all(
        isinstance(dtype, pd.StringDtype) and dtype.storage == "pyarrow"
        for dtype in frame.dtypes
    )
    assert extractor.call_count == (year == 2020)
    assert all(not root.exists() for root in extraction_roots)


def test_outer_join_preserves_missing_suppressed_and_zero_values() -> None:
    population = pd.DataFrame(
        {
            "cvegeo": ["01", "02"],
            "pob1": ["0", "*"],
            "_source_member_pob": ["pob.dbf"] * 2,
        },
        dtype=scince.STRING_DTYPE,
    )
    mortality = pd.DataFrame(
        {
            "cvegeo": ["02", "03"],
            "mor1_r": ["12.3400", ""],
            "_source_member_mor": ["mor.dbf"] * 2,
        },
        dtype=scince.STRING_DTYPE,
    )
    result = scince.combine_scince_themes(iter([population, mortality])).set_index(
        "cvegeo"
    )
    assert set(result.index) == {"01", "02", "03"}
    assert result.loc["01", "pob1"] == "0"
    assert result.loc["02", "pob1"] == "*"
    assert result.loc["02", "mor1_r"] == "12.3400"
    assert result.loc["03", "mor1_r"] == ""
    assert pd.isna(result.loc["03", "pob1"])
    assert pd.isna(result.loc["01", "_source_member_mor"])


def test_join_rejects_column_collisions_and_row_multiplication() -> None:
    frame = pd.DataFrame({"cvegeo": ["01"], "pob1": ["1"]}, dtype=scince.STRING_DTYPE)
    with pytest.raises(ValueError, match="columns overlap"):
        scince.combine_scince_themes([frame, frame])
    other = frame.rename(columns={"pob1": "mor1_r"})
    with pytest.raises(pd.errors.MergeError, match="not unique"):
        scince.combine_scince_themes([pd.concat([frame, frame]), other])


def test_empty_themes_preserve_schema() -> None:
    frame = pd.DataFrame(columns=["cvegeo", "pob1"], dtype=scince.STRING_DTYPE)
    other = pd.DataFrame(columns=["cvegeo", "mor1_r"], dtype=scince.STRING_DTYPE)
    result = scince.combine_scince_themes([frame, other])
    assert result.empty
    assert set(result.columns) == {"cvegeo", "pob1", "mor1_r"}


def test_installer_failure_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_archive(tmp_path, 2020, "ageb")
    monkeypatch.setattr(
        scince.subprocess,
        "run",
        Mock(side_effect=subprocess.CalledProcessError(1, "innoextract")),
    )
    with pytest.raises(subprocess.CalledProcessError):
        scince.read_scince_archive(tmp_path, 2020, "01", "ageb")


def test_load_combines_states_without_changing_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def read(directory: Path, year: int, state: str, level: str) -> pd.DataFrame:
        calls.append((directory, year, state, level))
        return pd.DataFrame(
            {"cvegeo": [state], "pob1": ["0042"]}, dtype=scince.STRING_DTYPE
        )

    monkeypatch.setattr(scince, "read_scince_archive", read)
    result = scince.load_scince(tmp_path, 2020, "ent")
    assert result["cvegeo"].tolist() == [f"{i:02d}" for i in range(1, 33)]
    assert result["pob1"].tolist() == ["0042"] * 32
    assert calls == [(tmp_path, 2020, f"{i:02d}", "ent") for i in range(1, 33)]
