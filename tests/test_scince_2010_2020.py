from collections.abc import Iterator
from typing import Literal

import pandas as pd
import pytest

import dagster as dg
from census_processing.defs.assets.scince.common import common_2010_2020
from census_processing.defs.assets.scince.scince_2010 import (
    SUPPLEMENTARY_TABLES_LABEL_MAP as TABLES_2010,
)
from census_processing.defs.assets.scince.scince_2010 import (
    scince_2010_ageb_pop,
)
from census_processing.defs.assets.scince.scince_2020 import (
    SUPPLEMENTARY_TABLES_LABEL_MAP as TABLES_2020,
)
from census_processing.defs.assets.scince.scince_2020 import scince_2020
from census_processing.defs.resources import ScinceColumnsToKeepResource


def process_op_factory(
    year: Literal[2010, 2020], suffixes: list[str]
) -> dg.OpDefinition:
    @dg.op(
        name=f"process_test_scince_{year}",
        ins={"scince_file_dep": dg.In(dagster_type=dg.Nothing)},
    )
    def process(state_code: str) -> dict[str, pd.DataFrame]:
        return {
            suffix: pd.DataFrame(
                {
                    "CVEGEO": [state_code],
                    "COUNT": [int(state_code) + index],
                    "KEEP_R": [1.5],
                    "DROP_R": [2.5],
                },
                index=[7],
            )
            for index, suffix in enumerate(suffixes)
        }

    return process


@pytest.mark.parametrize("population_only_2020", [False, True])
@pytest.mark.parametrize(
    "selection",
    [
        None,
        [dg.AssetKey(["staging", "2010", "scince", "pob"])],
        [
            dg.AssetKey(["staging", "2010", "scince", "disc"]),
            dg.AssetKey(["staging", "2010", "scince", "viv"]),
        ],
        [dg.AssetKey(["staging", "2020", "scince", "pob"])],
    ],
    ids=["all", "population_2010", "supplementary_2010", "population_2020"],
)
def test_scince_graphs_materialize_selected_tables(
    monkeypatch: pytest.MonkeyPatch,
    selection: list[dg.AssetKey] | None,
    *,
    population_only_2020: bool,
) -> None:
    @dg.op(out=dg.DynamicOut(str))
    def state_codes() -> Iterator[dg.DynamicOutput[str]]:
        for code in ("01", "02"):
            yield dg.DynamicOutput(code, mapping_key=code)

    monkeypatch.setattr(common_2010_2020, "get_state_codes", state_codes)
    suffixes_by_year: dict[Literal[2010, 2020], list[str]] = {
        2010: [*TABLES_2010, "pob"],
        2020: ["pob"] if population_only_2020 else [*TABLES_2020, "pob"],
    }
    assets = [
        common_2010_2020.scince_2010_2020_factory(
            year=year,
            process_op=process_op_factory(year, suffixes),
            table_suffixes=suffixes,
        )
        for year, suffixes in suffixes_by_year.items()
    ]
    result = dg.materialize(
        assets,
        selection=selection,
        resources={
            "io_manager": dg.mem_io_manager,
            "postgres_manager": dg.mem_io_manager,
            "scince_columns_to_keep_resource": ScinceColumnsToKeepResource(
                columns_2010=["keep_r"], columns_2020=["keep_r"]
            ),
        },
    )

    assert result.success
    production_keys = {
        dg.AssetKey(["staging", str(year), "scince", suffix])
        for year, suffixes in {
            2010: [*TABLES_2010, "pob"],
            2020: [*TABLES_2020, "pob"],
        }.items()
        for suffix in suffixes
    }
    assert scince_2010_ageb_pop.keys | scince_2020.keys == production_keys
    assert scince_2010_ageb_pop.can_subset
    assert scince_2020.can_subset
    expected_keys = {
        dg.AssetKey(["staging", str(year), "scince", suffix])
        for year, suffixes in suffixes_by_year.items()
        for suffix in suffixes
    }
    if selection is not None:
        expected_keys = set(selection)
    assert {
        event.asset_key for event in result.get_asset_materialization_events()
    } == expected_keys

    for asset, (year, suffixes) in zip(assets, suffixes_by_year.items(), strict=True):
        for index, suffix in enumerate(suffixes):
            key = dg.AssetKey(["staging", str(year), "scince", suffix])
            if key not in expected_keys:
                continue
            assert asset.get_io_manager_key_for_asset_key(key) == "postgres_manager"
            assert asset.metadata_by_key[key] == {
                "schema": "staging",
                "table": f"scince_{year}_ageb_{suffix}_prepared",
            }
            actual = result.output_for_node(
                f"scince_{year}_ageb.concat_frame_maps_{year}", suffix
            )
            pd.testing.assert_frame_equal(
                actual,
                pd.DataFrame(
                    {
                        "cvegeo": ["01", "02"],
                        "count": [1 + index, 2 + index],
                        "keep_r": [1.5, 1.5],
                    }
                ),
            )
