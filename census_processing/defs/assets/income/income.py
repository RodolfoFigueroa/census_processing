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
    context: dg.OpExecutionContext,
    df_survey: pd.DataFrame,
    df_census: pd.DataFrame,
    df_census_geometry: gpd.GeoDataFrame,
) -> dict:
    seed_xr = ipf.generate_contingency_table(df_survey, INCOME_LINKING_COLS)
    ds = ipf.apply_ipf(df_census, seed_xr)
    df_ind = ipf.generate_individual_weights(df_survey, ds)

    pop_income = ipf.get_income_df(ds, df_census, df_census_geometry, df_ind)[
        ["cvegeo", "income", "income_pc"]
    ]

    h, df_cdf, _, _, _ = seg.global_H_index(df_ind, df_census.index.to_list())
    out_df = (
        pop_income.set_index("cvegeo")
        .assign(gini=calculate_local_gini(df_cdf.reset_index()))
        .reset_index()
    )
    return {"H": h, "income": out_df, "mapping_key": context.get_mapping_key()}


@dg.graph
def full_graph(met_zone: str, agebs_2020: None, muns_2020: None) -> dict:
    df_census = load_census(met_zone, agebs_2020)
    df_census_geometries = load_census_geometries(met_zone, agebs_2020)
    df_survey = load_survey(met_zone, muns_2020)
    return calculate_income(df_survey, df_census, df_census_geometries)


@dg.op(
    out={
        "income": dg.Out(io_manager_key="postgres_manager"),
        "H": dg.Out(io_manager_key="postgres_manager"),
    }
)
def concat_results(results_list: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_list, h_map = [], {}
    for submap in results_list:
        df_list.append(submap["income"])
        h_map[submap["mapping_key"]] = submap["H"]

    df_income = pd.concat(df_list, ignore_index=True)
    df_h = (
        pd.Series(h_map, name="H")
        .to_frame()
        .reset_index(names="cve_met")
        .assign(cve_met=lambda df: df["cve_met"].str.replace("_", "."))
    )
    return df_income, df_h


@dg.graph_multi_asset(
    ins={
        "agebs_2020": dg.AssetIn(
            key=["census", "2020", "ageb"], dagster_type=dg.Nothing
        ),
        "muns_2020": dg.AssetIn(key=["census", "2020", "mun"], dagster_type=dg.Nothing),
        "metropoli_2020": dg.AssetIn(
            key=["metropoli", "2020"], dagster_type=dg.Nothing
        ),
    },
    outs={
        "income": dg.AssetOut(
            key="income",
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
        ),
        "H": dg.AssetOut(
            key="H",
            metadata={
                "primary_key": "cve_met",
                "table_name": "segregation_met_zone",
                "foreign_keys": [
                    {
                        "column": "cve_met",
                        "ref_table": "metropoli_2020",
                        "ref_column": "cve_met",
                    }
                ],
            },
        ),
    },
    group_name="income",
)
def income(
    agebs_2020: None, muns_2020: None, metropoli_2020: None
) -> dict[str, pd.DataFrame]:
    met_zones = get_met_zones(metropoli_2020)
    df_income, df_h = concat_results(
        met_zones.map(
            lambda met_zone: full_graph(met_zone, agebs_2020, muns_2020)
        ).collect()
    )
    return {
        "income": df_income,
        "H": df_h,
    }
