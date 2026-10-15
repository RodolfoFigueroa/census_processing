import pytest

from census_processing.defs.assets.scince.common.common_1990_2000 import (
    concat_and_clean_scince_dataframes,
)
from census_processing.defs.assets.scince.scince_2000 import (
    chunk_to_dataframe,
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
