import pytest

from census_processing.defs.assets.scince.common.common_1990_2000 import (
    concat_and_clean_scince_dataframes,
)
from census_processing.defs.assets.scince.scince_2000 import (
    chunk_to_dataframe,
    extract_state_records,
    process_chunk,
)


@pytest.mark.parametrize("missing_marker", ["N.D.", "*"])
def test_all_missing_indicators_preserve_output_schema(missing_marker: str) -> None:
    records = []
    for ageb in ("001-0", "002-A"):
        indicators = [
            missing_marker if i in (98, 99) else str(i) for i in range(1, 171)
        ]
        records.append(" ".join([ageb, *indicators]))

    chunk = chunk_to_dataframe(process_chunk(" ".join(records)))
    output = concat_and_clean_scince_dataframes([chunk])

    assert output.columns.tolist() == ["cvegeo", *[f"pob{i}" for i in range(1, 171)]]
    assert output["cvegeo"].tolist() == ["0010", "002A"]
    assert output[["pob98", "pob99"]].isna().all().all()
    for i in range(1, 171):
        if i not in (98, 99):
            assert output[f"pob{i}"].tolist() == [i, i]


@pytest.mark.parametrize(("header", "width"), [(8277, 1718), (5505, 1706)])
def test_state_fixed_width_records(header: int, width: int) -> None:
    buffer = bytearray(header)
    buffer[4:8] = (2).to_bytes(4, "little")
    buffer[8:10] = header.to_bytes(2, "little")
    buffer[10:12] = width.to_bytes(2, "little")
    values = ["12"] * 170
    values[47] = "1.25"
    values[97] = "N.D."
    for key in ("01001", "000"):
        buffer.extend(
            (
                " " + key.ljust(width - 1701) + "".join(v.ljust(10) for v in values)
            ).encode()
        )
    frame = extract_state_records(bytes(buffer), "01")
    assert frame.index.tolist() == ["0100100000000", "0100000000000"]
    clean = concat_and_clean_scince_dataframes([frame])
    assert clean["pob48"].tolist() == [1.25, 1.25]
    assert clean["pob98"].isna().all()
    with pytest.raises(ValueError, match="Truncated"):
        extract_state_records(bytes(buffer[:-1]), "01")
