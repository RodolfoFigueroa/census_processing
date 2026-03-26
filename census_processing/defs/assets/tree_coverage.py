import ee
import geemap
import geopandas as gpd
import pandas as pd
from dagster_components.resources import PostGISResource

import dagster as dg

ee.Initialize()


@dg.op(ins={"df_agebs": dg.In(dagster_type=dg.Nothing)})
def get_cvegeo_chunks(postgis_resource: PostGISResource) -> list[list[str]]:
    with postgis_resource.connect() as conn:
        cvegeo_flat = pd.read_sql(
            "SELECT cvegeo FROM census_2020_ageb ORDER BY cvegeo", conn
        )
    return [
        cvegeo_flat["cvegeo"].iloc[i : i + 1000].tolist()
        for i in range(0, len(cvegeo_flat), 1000)
    ]


@dg.op(ins={"df_agebs": dg.In(dagster_type=dg.Nothing)})
def get_all_agebs_bbox(postgis_resource: PostGISResource) -> ee.Geometry:
    with postgis_resource.connect() as conn:
        bounds_series: pd.Series = pd.read_sql(
            """
        SELECT 
        ST_Xmin(bbox) AS xmin,
        ST_Xmax(bbox) AS xmax,
        ST_Ymin(bbox) AS ymin,
        ST_Ymax(bbox) AS ymax
        FROM (
            SELECT ST_Extent(ST_Transform(geometry, 4326)) AS bbox 
                FROM census_2020_ageb
        )
        """,
            conn,
        ).iloc[0]

    return ee.Geometry.BBox(
        bounds_series["xmin"],
        bounds_series["ymin"],
        bounds_series["xmax"],
        bounds_series["ymax"],
    )


@dg.op(out=dg.Out(io_manager_key="dataframe_postgres_manager"))
def process_chunks(
    context: dg.OpExecutionContext,
    postgis_resource: PostGISResource,
    bbox: ee.Geometry,
    chunks: list[list[str]],
) -> pd.DataFrame:
    img_coverage = (
        ee.ImageCollection(
            "projects/sat-io/open-datasets/facebook/meta-canopy-height",
        )
        .filterBounds(bbox)
        .mean()
        .gte(ee.Number(3))
        .multiply(ee.image.Image.pixelArea())
    )

    computed_list = []
    for chunkl in chunks:
        chunk = tuple(chunkl)

        with postgis_resource.connect() as conn:
            df_chunk = gpd.read_postgis(
                """
                    SELECT cvegeo, ST_Transform(geometry, 4326) AS geometry
                    FROM census_2020_ageb
                    WHERE cvegeo IN %(chunk)s
                    """,
                conn,
                params={"chunk": chunk},
                geom_col="geometry",
            )  # ty:ignore[no-matching-overload]

        features = geemap.geopandas_to_ee(df_chunk)
        computed = ee.data.computeFeatures(
            {
                "expression": (
                    img_coverage.reduceRegions(
                        features, reducer=ee.Reducer.sum(), scale=25
                    )
                ),
                "fileFormat": "PANDAS_DATAFRAME",
            },
        )

        # TODO: Temporary fix until ty respects annotated over inferred types
        computed = gpd.GeoDataFrame(computed)[["cvegeo", "sum"]]
        computed_list.append(computed)

    context.log.info("All chunks processed. Combining results...")
    return pd.concat(computed_list, ignore_index=True)


@dg.graph_asset(
    key=["tree_coverage", "2020", "ageb"],
    ins={
        "df_agebs": dg.AssetIn(key=["census", "2020", "ageb"], dagster_type=dg.Nothing)
    },
    group_name="tree_coverage_2020",
    metadata={"table_name": "tree_coverage_2020_ageb", "primary_key": "cvegeo"},
)
def tree_coverage_2020_ageb(df_agebs: None) -> pd.DataFrame:
    bbox_global = get_all_agebs_bbox(df_agebs)
    cvegeo_chunks = get_cvegeo_chunks(df_agebs)
    return process_chunks(bbox_global, cvegeo_chunks)


# @dg.asset(
#     ins={
#         "df_agebs": dg.AssetIn(
#             key=["census", "2020", "ageb"], metadata={"columns": ["cvegeo", "geometry"]}
#         )
#     }
# )
# def tree_coverage(context: dg.AssetExecutionContext, df_agebs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
#     bbox = shapely.box(*df_agebs.to_crs("EPSG:4326").total_bounds)
#     bbox_ee = ee.geometry.Geometry.Polygon(
#         list(zip(*bbox.exterior.coords.xy, strict=True))
#     )

#     chunk_size = 1000
#     chunks = [
#         df_metropoli[["CVEGEO", "geometry"]]
#         .to_crs("EPSG:4326")
#         .iloc[i : i + chunk_size]
#         for i in range(0, len(df_metropoli), chunk_size)
#     ]

#     computed_chunks = []
#     for chunk in chunks:
#         features = geemap.geopandas_to_ee(chunk)
#         computed = ee.data.computeFeatures(
#             {
#                 "expression": (
#                     ee.ImageCollection(
#                         "projects/sat-io/open-datasets/facebook/meta-canopy-height",
#                     )
#                     .filterBounds(bbox_ee)
#                     .mean()
#                     .gte(ee.Number(3))
#                     .multiply(ee.image.Image.pixelArea())
#                     .reduceRegions(features, reducer=ee.Reducer.sum(), scale=10)
#                 ),
#                 "fileFormat": "GEOPANDAS_GEODATAFRAME",
#             },
#         )
#         computed_chunks.append(computed)

#     df_metropoli = (
#         df_metropoli.merge(
#             pd.concat(computed_chunks)[["CVEGEO", "sum"]],
#             on="CVEGEO",
#             how="left",
#         )
#         .rename(columns={"sum": "tree_canopy_area_m2"})
#         .assign(
#             tree_canopy_area_frac=lambda df: (
#                 df["tree_canopy_area_m2"] / df["geometry"].area
#             ),
#         )
#     )
