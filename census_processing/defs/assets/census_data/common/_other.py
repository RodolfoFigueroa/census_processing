import geopandas as gpd
import pandas as pd
import shapely
from cfc_dagster_utils.utils import cast_all_columns_to_numeric

import dagster as dg


@dg.op
def add_higher_levels_cvegeo(df: pd.DataFrame) -> pd.DataFrame:
    cvegeo_length = df["cvegeo"].str.len().iloc[0]

    if cvegeo_length == 2:
        err = "cvegeo is already at the highest level (entidad)"
        raise ValueError(err)

    df = df.assign(cve_ent=lambda df: df["cvegeo"].str[:2])

    if cvegeo_length >= 9:
        df = df.assign(cve_mun=lambda df: df["cvegeo"].str[:5])

    if cvegeo_length >= 13:
        df = df.assign(cve_loc=lambda df: df["cvegeo"].str[:9])

    if cvegeo_length == 16:
        df = df.assign(cve_ageb=lambda df: df["cvegeo"].str[:13])

    return df


@dg.op(
    ins={
        "census": dg.In(),
        "geometry": dg.In(),
        "ent_dep": dg.In(dagster_type=dg.Nothing),
        "mun_dep": dg.In(dagster_type=dg.Nothing),
        "loc_dep": dg.In(dagster_type=dg.Nothing),
        "ageb_dep": dg.In(dagster_type=dg.Nothing),
    },
    out=dg.Out(io_manager_key="postgres_manager"),
)
def merge_census_and_geometry(
    census: pd.DataFrame,
    geometry: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    return (
        geometry.merge(census, on="cvegeo", how="inner")
        .pipe(
            lambda df: cast_all_columns_to_numeric(
                df,
                ignore=[
                    "cvegeo",
                    "cve_ent",
                    "cve_mun",
                    "cve_loc",
                    "cve_ageb",
                    "cve_met",
                    "nom_ent",
                    "nom_mun",
                    "nom_loc",
                    "geometry",
                ],
                errors="coerce",
            )
        )
        .pipe(gpd.GeoDataFrame, geometry="geometry", crs=geometry.crs)
    )


@dg.op(out=dg.Out(io_manager_key="postgres_manager"))
def add_dummy_geometry(df: pd.DataFrame) -> gpd.GeoDataFrame:
    return df.assign(
        geometry=lambda df: shapely.empty(
            len(df),
            geom_type=shapely.GeometryType.POLYGON,
        ),
    ).pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:6372"))


@dg.op
def get_loc_geometry_from_agebs(ageb_geometries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return (
        ageb_geometries.assign(cvegeo=lambda df: df["cvegeo"].str[:9])
        .dissolve(
            by="cvegeo",
        )[["geometry"]]
        .reset_index()
    )
