import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.constants import INCOME_LINKING_COLS
from census_processing.defs.assets.income import ipf, seg
from census_processing.defs.assets.income.census import (
    load_census,
    load_census_geometries,
)
from census_processing.defs.assets.income.survey import load_survey
from census_processing.defs.assets.income.zones import get_met_zones


def gini(accum_pop: list, accum_income: list) -> float:
    if len(accum_pop) != len(accum_income):
        err = f"Length mismatch: {len(accum_pop)} vs {len(accum_income)}"
        raise ValueError(err)

    s = 0
    for i in range(1, len(accum_pop)):
        s += (accum_pop[i] - accum_pop[i - 1]) * (accum_income[i] + accum_income[i - 1])
    return 1 - s


def calculate_local_gini(df_cdf: pd.DataFrame) -> pd.Series:
    df_fixed = (
        df_cdf.drop(columns=["w_MZ"])
        .assign(
            Ingreso_orig=lambda x: (
                x["Ingreso_orig"] / x["Ingreso_orig"].sum()
            ).cumsum(),
        )
        .set_index("Ingreso_orig")
    )
    df_fixed.loc[0] = 0
    df_fixed = df_fixed.sort_index()

    return df_fixed.apply(lambda x: gini(x.to_numpy(), x.index.to_numpy()))


@dg.op
def calculate_income(
    df_survey: pd.DataFrame,
    df_census: pd.DataFrame,
    df_census_geometry: gpd.GeoDataFrame,
) -> pd.DataFrame:
    seed_xr = ipf.generate_contingency_table(df_survey, INCOME_LINKING_COLS)
    ds = ipf.apply_ipf(df_census, seed_xr)
    df_ind = ipf.generate_individual_weights(df_survey, ds)

    pop_income = ipf.get_income_df(ds, df_census, df_census_geometry, df_ind)[
        ["cvegeo", "income", "income_pc"]
    ]

    _, df_cdf, _, _, _ = seg.global_H_index(df_ind, df_census.index.to_list())
    return (
        pop_income.set_index("cvegeo")
        .assign(gini=calculate_local_gini(df_cdf.reset_index()))
        .reset_index()
    )


@dg.graph
def full_graph(met_zone: str, agebs_2020: None) -> pd.DataFrame:
    df_census = load_census(met_zone, agebs_2020)
    df_census_geometries = load_census_geometries(met_zone, agebs_2020)
    df_survey = load_survey(met_zone)
    return calculate_income(df_survey, df_census, df_census_geometries)


@dg.op(out=dg.Out(io_manager_key="dataframe_postgres_manager"))
def concat_results(df_list: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(df_list, ignore_index=True)


@dg.graph_asset(
    key="income",
    ins={
        "agebs_2020": dg.AssetIn(
            key=["census", "2020", "ageb"], dagster_type=dg.Nothing
        )
    },
    group_name="income",
    metadata={
        "primary_key": "cvegeo",
        "table_name": "income_2020",
        "foreign_keys": [
            {
                "column": "cvegeo",
                "ref_table": "census_2020_ageb",
                "ref_column": "cvegeo",
            }
        ],
    },
)
def income(agebs_2020: None) -> pd.DataFrame:
    met_zones = get_met_zones()
    return concat_results(
        met_zones.map(lambda met_zone: full_graph(met_zone, agebs_2020)).collect()
    )
