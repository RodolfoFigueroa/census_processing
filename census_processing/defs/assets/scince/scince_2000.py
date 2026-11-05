import tempfile
import zipfile
from pathlib import Path

import cabarchive
import numpy as np
import pandas as pd
import rarfile

import dagster as dg
from census_processing.defs.assets.scince.common.common_1990_2000 import (
    scince_1990_2000_factory,
)
from census_processing.defs.resources import PathResource


def get_names_from_archive(arc: cabarchive.CabArchive) -> list[str]:
    """
    Extract and clean the variable names from the archive.

    Args:
        arc: The CAB archive object.

    Returns:
        A list of cleaned variable names.
    """
    names = None
    for key in arc:
        if "CGPV2000" in key:
            names = (
                arc[key]
                .buf.decode("latin1")
                .replace("\x03e", "")
                .replace("\x1bª", "")
                .replace("\x00", "")
                .replace("\x1a", "")
                .replace("¢", "ó")
                .replace("¤", "ñ")
                .replace("\xa0", "á")
                .replace("\x82", "é")
                .strip()
            )
            break

    if names is None:
        err = "CGPV2000 file not found in archive"
        raise ValueError(err)

    split = names.split("\r")[1].strip()
    return [split[353 * i : 353 * (i + 1)].strip() for i in range(170)]


def decode_chunk(buffer: bytes) -> str:
    """
    Decode a chunk of bytes into a clean string.

    Args:
        buffer: The byte buffer to decode.

    Returns:
        The cleaned string.
    """
    return (
        buffer.decode("latin1")
        .split("\n")[-1]
        .replace("\x00", "")
        .replace("\x1a", "")
        .replace("\r", "")
        .strip()
    )


def fix_merged_element(elem: str, affix: str) -> list:
    """
    Fix elements that have merged with affixes like "N.D." or "*".

    Args:
        elem: The element to fix.
        affix: The affix to split on.

    Returns:
        A list of fixed elements, with np.nan for missing values.
    """
    out = []
    if elem == affix:
        return [np.nan]

    split = elem.split(affix)
    for s in split:
        if len(s) == 0:
            out.append(np.nan)
        else:
            out.append(s)
    return out


def process_chunk(chunk: str) -> list[str]:
    """
    Process a chunk of data, handling special cases.

    Args:
        chunk: The chunk of data to process.

    Returns:
        A list of processed elements.
    """
    elems = []
    for elem in chunk.split():
        if "-" in elem:
            a, b = elem.split("-")
            ageb_code = a + b[0]
            z1 = b[1:]
            elems.append(ageb_code)
            elems.append(z1)
        elif "N.D." in elem:
            fixed = fix_merged_element(elem, "N.D.")
            elems.extend(fixed)
        elif "*" in elem:
            fixed = fix_merged_element(elem, "*")
            elems.extend(fixed)
        else:
            elems.append(elem)

    return [x for x in elems if x != ""]


def chunk_to_dataframe(rows: list[str]) -> pd.DataFrame:
    """
    Convert a list of rows into a structured DataFrame.

    Args:
        rows: The list of rows to convert.

    Returns:
        The structured DataFrame.
    """
    if len(rows) % 171 != 0:
        err = "Number of elements is not a multiple of 171"
        raise ValueError(err)

    return (
        pd.Series(rows)
        .to_frame()
        .assign(
            row_idx=lambda df: df.index // 171,
            col_idx=lambda df: df.index % 171,
        )
        .pivot_table(
            index="row_idx", columns="col_idx", values=0, aggfunc="first", dropna=False
        )
        .set_index(0)
        .astype(
            {98: "Int32", 99: "Int32"}
        )  # Fix for all-null columns at the AGEB level
    )


def extract_state_records(buffer: bytes, state: str) -> pd.DataFrame:
    """Read the two published fixed-width state DBP layouts."""
    count = int.from_bytes(buffer[4:8], "little")
    header = int.from_bytes(buffer[8:10], "little")
    width = int.from_bytes(buffer[10:12], "little")
    if (header, width) not in {(8277, 1718), (5505, 1706)}:
        msg = "Unsupported state DBP layout"
        raise ValueError(msg)
    if len(buffer) < header + count * width:
        msg = "Truncated state DBP records"
        raise ValueError(msg)
    key_width = width - 1701
    rows = []
    keys = []
    for offset in range(header, header + count * width, width):
        record = buffer[offset : offset + width]
        if record[:1] != b" ":
            msg = "Unexpected state DBP record marker"
            raise ValueError(msg)
        key = record[1 : 1 + key_width].decode("latin1").strip()
        if key == "000":
            key = state
        elif not (
            len(key) == 5
            and key.isdigit()
            and key.startswith(state)
            and key[2:] != "000"
        ):
            msg = "Invalid state DBP geographic identifier"
            raise ValueError(msg)
        keys.append(key.ljust(13, "0"))
        payload = record[1 + key_width :]
        rows.append(
            [payload[i : i + 10].decode("latin1").strip() for i in range(0, 1700, 10)]
        )
    return pd.DataFrame(rows, index=keys, columns=range(1, 171))


def extract_from_archive(arc: cabarchive.CabArchive) -> pd.DataFrame:
    """
    Extract and process census data from a CAB archive.

    Args:
        arc: The CAB archive object.

    Returns:
        The processed census DataFrame.
    """

    wanted_keys = []
    for elem in arc:
        if elem.upper().endswith(".DBP"):
            suffix = elem.rsplit("_", 1)[-1][:-4]
            if suffix.isdigit() and len(suffix) in (2, 9):
                wanted_keys.append(elem)

    df_census = []
    for key in wanted_keys:
        loc_code = key.rsplit("_", 1)[-1][:-4]
        buf = arc[key].buf
        if len(loc_code) == 2:
            try:
                df_census.append(extract_state_records(buf, loc_code))
            except ValueError as exc:
                msg = f"Invalid SCINCE member {key}: {exc}"
                raise ValueError(msg) from exc
            continue
        decoded = decode_chunk(buf)
        processed = process_chunk(decoded)
        df_chunk = chunk_to_dataframe(processed)
        df_chunk.index = loc_code + df_chunk.index.astype(str)
        df_census.append(df_chunk)

    result = pd.concat(df_census)
    if result.index.has_duplicates:
        msg = "Duplicate SCINCE geographic identifiers"
        raise ValueError(msg)
    return result


@dg.op(ins={"scince_file_dep": dg.In(dagster_type=dg.Nothing)})
def load_scince_2000(path_resource: PathResource) -> list[pd.DataFrame]:
    raw_path = Path(path_resource.in_path)

    df_census_list: list[pd.DataFrame] = []
    with (
        zipfile.ZipFile(raw_path / "2000" / "censo2000_scince.zip") as f,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        f.extractall(tmpdir)
        extracted_path = Path(tmpdir) / "censo2000_scince"

        for rar in extracted_path.glob("*.rar"):
            with (
                rarfile.RarFile(rar) as rf,
                tempfile.TemporaryDirectory() as rar_tmpdir,
            ):
                rf.extractall(rar_tmpdir)

                cab_path = Path(rar_tmpdir) / rar.stem / "Data.Cab"
                with cab_path.open("rb") as cabf:
                    arc = cabarchive.CabArchive(cabf.read())

            df_census_list.append(extract_from_archive(arc))

    if pd.concat(df_census_list).index.has_duplicates:
        msg = "Duplicate SCINCE geographic identifiers across archives"
        raise ValueError(msg)
    return df_census_list


scince_2000 = scince_1990_2000_factory(year=2000, load_scince_op=load_scince_2000)
