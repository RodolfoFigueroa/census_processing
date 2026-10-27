import struct
import subprocess
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace
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
    return scince.read_scince_dbf(
        path, source_file="state.zip", source_member="01/attributes.dbf"
    )


@pytest.mark.parametrize(
    "value", ["", "*", "****", "N/D", "12,5", "* 12.50 *", "9007199254740993"]
)
def test_raw_values_remain_text(tmp_path: Path, value: str) -> None:
    frame = read_fixture(tmp_path, dbf_bytes(rows=[(IDENTIFIER, value, "12.34000000")]))
    assert frame.iloc[0].tolist() == [
        IDENTIFIER,
        value,
        "12.34000000",
        "state.zip",
        "01/attributes.dbf",
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
        *scince.PROVENANCE_COLUMNS,
    ]
    assert frame.loc[0, "grado_marg"] == "  Muy bajo á"


def test_empty_table_preserves_headers_and_types(tmp_path: Path) -> None:
    frame = read_fixture(tmp_path, dbf_bytes(rows=[]))
    assert frame.empty
    assert frame.columns.tolist() == [
        "cvegeo",
        "pob1",
        "pob1_r",
        *scince.PROVENANCE_COLUMNS,
    ]
    assert all(isinstance(dtype, pd.StringDtype) for dtype in frame.dtypes)


@pytest.mark.parametrize(
    "identifier",
    ["", "10010001001A", "010010001001AA", "01001000100-A", "ABCDEFGHIJKLM"],
)
def test_invalid_identifiers_fail(tmp_path: Path, identifier: str) -> None:
    with pytest.raises(ValueError, match="Invalid cvegeo"):
        read_fixture(tmp_path, dbf_bytes((("CVEGEO", "C", 20),), [(identifier,)]))


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ((("CVEGEO", "C", 13), ("cvegeo", "C", 13)), "Duplicate normalized"),
        ((("POB1", "N", 10),), "Missing cvegeo"),
        ((("CVEGEO", "C", 13), ("DATE", "D", 8)), "Unsupported DBF type D"),
    ],
)
def test_bad_headers_fail(
    tmp_path: Path, fields: Sequence[tuple[str, str, int]], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        read_fixture(tmp_path, dbf_bytes(fields, []))


def test_reserved_provenance_names_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = SimpleNamespace(
        fields=[SimpleNamespace(name=n) for n in ("CVEGEO", "_source_file")]
    )
    monkeypatch.setattr(scince, "DBF", Mock(return_value=table))
    with pytest.raises(ValueError, match="reserved provenance"):
        read_fixture(tmp_path, b"")


def theme_fixture(theme: str) -> bytes:
    if theme == "mor":
        return dbf_bytes(
            (("CVEGEO", "C", 13), ("MOR1_R", "N", 12)), [(IDENTIFIER, "5.2500")]
        )
    if theme == "des":
        return dbf_bytes((("CVEGEO", "C", 13), ("GRADO_MARG", "C", 8)), [])
    return dbf_bytes()


def write_archive(directory: Path, year: int) -> None:
    name = "01_SCINCE_zip.zip" if year == 2010 else "SCINCE2020_DATOS_01.zip"
    with ZipFile(directory / name, "w") as archive:
        if year == 2010:
            for theme, relative in scince.SCINCE_TABLES[year].items():
                archive.writestr("01/" + relative.upper(), theme_fixture(theme))
        else:
            archive.writestr("nested/SCINCE.EXE", b"installer fixture")


@pytest.mark.parametrize("year", [2010, 2020])
def test_archive_layouts_and_theme_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, year: int
) -> None:
    write_archive(tmp_path, year)
    extraction_roots = []

    def extract(args: list[str], *, check: bool, stdout: int) -> None:
        assert args[0] == "/usr/bin/innoextract"
        assert Path(args[1]).read_bytes() == b"installer fixture"
        assert args[2] == "-d"
        assert check
        assert stdout == subprocess.DEVNULL
        root = Path(args[3])
        extraction_roots.append(root)
        for theme, relative in scince.SCINCE_TABLES[2020].items():
            path = root / "app/01_Aguascalientes" / relative.upper()
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(theme_fixture(theme))

    extractor = Mock(side_effect=extract)
    monkeypatch.setattr(scince.subprocess, "run", extractor)
    frames = scince.read_scince_archive(tmp_path, year, "01")
    assert set(frames) == set(scince.SCINCE_TABLES[year])
    assert len(frames) == (14 if year == 2010 else 13)
    assert frames["mor"]["mor1_r"].tolist() == ["5.2500"]
    assert "pob1_r" in frames["pob"]
    if year == 2010:
        assert frames["des"].empty
    for frame in frames.values():
        assert (
            not frame["_source_member"].str.contains(str(tmp_path), regex=False).any()
        )
        assert not frame["_source_member"].str.startswith("/").any()
    prefix = "01/" if year == 2010 else "app/01_Aguascalientes/"
    assert frames["mor"]["_source_member"].tolist() == [
        prefix + scince.SCINCE_TABLES[year]["mor"].upper()
    ]
    assert extractor.call_count == (year == 2020)
    assert all(not root.exists() for root in extraction_roots)


@pytest.mark.parametrize(
    ("year", "state"), [(2000, "01"), (2010, "00"), (2020, "33"), (2020, "1")]
)
def test_invalid_archive_arguments(tmp_path: Path, year: int, state: str) -> None:
    with pytest.raises(ValueError, match=r"Unsupported SCINCE year|Invalid state code"):
        scince.read_scince_archive(tmp_path, year, state)


def test_installer_failure_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_archive(tmp_path, 2020)
    monkeypatch.setattr(
        scince.subprocess,
        "run",
        Mock(side_effect=subprocess.CalledProcessError(1, "innoextract")),
    )
    with pytest.raises(subprocess.CalledProcessError):
        scince.read_scince_archive(tmp_path, 2020, "01")


@pytest.mark.parametrize("installers", [[], ["one.exe", "two.EXE"]])
def test_missing_or_ambiguous_installer(tmp_path: Path, installers: list[str]) -> None:
    with ZipFile(tmp_path / "SCINCE2020_DATOS_01.zip", "w") as archive:
        for name in installers:
            archive.writestr(name, b"")
    with pytest.raises(ValueError, match="exactly one SCINCE installer"):
        scince.read_scince_archive(tmp_path, 2020, "01")


@pytest.mark.parametrize("members", [[], ["ageb_urb.dbf", "AGEB_URB.DBF"]])
def test_missing_or_ambiguous_dbf(tmp_path: Path, members: list[str]) -> None:
    with ZipFile(tmp_path / "01_SCINCE_zip.zip", "w") as archive:
        archive.writestr("01/", b"")
        for name in members:
            archive.writestr("01/" + name, dbf_bytes())
    with pytest.raises(ValueError, match=r"exactly one member ageb_urb\.dbf"):
        scince.read_scince_archive(tmp_path, 2010, "01")


def test_concatenation_aligns_columns_and_preserves_rows(tmp_path: Path) -> None:
    frame = read_fixture(tmp_path, dbf_bytes())
    maps = [
        {"pob": frame, "des": frame.iloc[:0]},
        {"pob": frame[frame.columns[::-1]], "des": frame},
    ]
    result = scince.concatenate_scince_theme(maps, "pob")
    assert result.columns.tolist() == frame.columns.tolist()
    assert result["cvegeo"].tolist() == [IDENTIFIER, IDENTIFIER]
    assert result.index.tolist() == [0, 1]
    assert len(scince.concatenate_scince_theme(maps, "des")) == 1


def test_schema_mismatch_identifies_state_and_theme(tmp_path: Path) -> None:
    frame = read_fixture(tmp_path, dbf_bytes())
    with pytest.raises(ValueError, match="state 02, theme pob"):
        scince.concatenate_scince_theme(
            [{"pob": frame}, {"pob": frame.drop(columns="pob1_r")}], "pob"
        )


def test_empty_state_collection_fails() -> None:
    with pytest.raises(ValueError, match="No state frames"):
        scince.concatenate_scince_theme([], "pob")
