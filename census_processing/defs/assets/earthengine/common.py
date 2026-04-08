import ee
import geemap
import geopandas as gpd
import pandas as pd
from dagster_components.resources import PostGISResource

import dagster as dg


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
        return gpd.GeoDataFrame(computed)[["cvegeo", "sum"]]

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
