from collections.abc import Iterator

import ee
import geemap
import geopandas as gpd
import pandas as pd
from dagster_components.resources import PostGISResource

import dagster as dg


@dg.op(ins={"df_agebs": dg.In(dagster_type=dg.Nothing)}, out=dg.DynamicOut())
def get_cvegeo_chunks(
    postgis_resource: PostGISResource,
) -> Iterator[dg.DynamicOutput[list[str]]]:
    with postgis_resource.connect() as conn:
        cvegeo_flat = pd.read_sql(
            "SELECT cvegeo FROM census_2020_ageb ORDER BY cvegeo", conn
        )

    for i in range(0, len(cvegeo_flat), 1000):
        yield dg.DynamicOutput(
            cvegeo_flat["cvegeo"].iloc[i : i + 1000].tolist(), mapping_key=str(i)
        )


def process_cvegeo_chunk_factory(
    name: str, *, reducer: ee.Reducer, scale: int, table_name: str
) -> dg.OpDefinition:
    @dg.op(name=name)
    def _op(
        postgis_resource: PostGISResource, chunkl: list[str], img: ee.Image
    ) -> pd.DataFrame:
        chunk = tuple(chunkl)

        with postgis_resource.connect() as conn:
            df_chunk = gpd.read_postgis(
                f"""
                    SELECT cvegeo, ST_Transform(geometry, 4326) AS geometry
                    FROM {table_name}
                    WHERE cvegeo IN %(chunk)s
                    """,  # noqa: S608
                conn,
                params={"chunk": chunk},
                geom_col="geometry",
            )  # ty:ignore[no-matching-overload]

        features = geemap.geopandas_to_ee(df_chunk)
        computed = ee.data.computeFeatures(
            {
                "expression": (
                    img.reduceRegions(features, reducer=reducer, scale=scale)
                ),
                "fileFormat": "PANDAS_DATAFRAME",
            },
        )

        # TODO: Temporary fix until ty respects annotated over inferred types
        gdf = gpd.GeoDataFrame(computed)
        return gdf[["cvegeo", "sum"]].rename(columns={"sum": "area_m2"})

    return _op


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
def concat_processed_chunks(chunks: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(chunks, ignore_index=True)


def reduce_ee_image_factory(
    *,
    reducer: ee.Reducer,
    scale: int,
    table_name: str,
    img_op: dg.OpDefinition,
    decorator_kwargs: dict,
) -> dg.AssetsDefinition:
    op_name = "_".join(decorator_kwargs["key"]) + "_chunk_processor"
    process_op = process_cvegeo_chunk_factory(
        op_name, reducer=reducer, scale=scale, table_name=table_name
    )

    @dg.graph_asset(**decorator_kwargs)
    def _asset(df_agebs: None) -> pd.DataFrame:
        bbox_global = get_all_agebs_bbox(df_agebs)
        img_coverage = img_op(bbox_global)
        cvegeo_chunks = get_cvegeo_chunks(df_agebs)

        return concat_processed_chunks(
            cvegeo_chunks.map(lambda chunk: process_op(chunk, img_coverage)).collect()
        )

    return _asset
