from collections.abc import Iterator

import ee
import pandas as pd
from dagster_components.resources import PostGISResource

import dagster as dg
from census_processing.defs.assets.earthengine.common import (
    get_all_agebs_bbox,
    process_cvegeo_chunk_factory,
)


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


@dg.op
def filter_coverage_img(bbox: ee.Geometry) -> ee.Image:
    return (
        ee.ImageCollection(
            "projects/sat-io/open-datasets/facebook/meta-canopy-height",
        )
        .filterBounds(bbox)
        .mean()
        .gte(ee.Number(3))
        .multiply(ee.image.Image.pixelArea())
    )


@dg.op(out=dg.Out(io_manager_key="dataframe_postgres_manager"))
def concat_processed_chunks(chunks: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(chunks, ignore_index=True)


process_tree_coverage_chunk = process_cvegeo_chunk_factory(
    name="process_tree_coverage_chunk",
    reducer=ee.Reducer.sum(),
    scale=25,
    table_name="census_2020_ageb",
)


@dg.graph_asset(
    key=["tree_coverage", "2020", "ageb"],
    ins={
        "df_agebs": dg.AssetIn(key=["census", "2020", "ageb"], dagster_type=dg.Nothing)
    },
    group_name="tree_coverage_2020",
    metadata={
        "table_name": "tree_coverage_2020_ageb",
        "primary_key": "cvegeo",
        "foreign_keys": [
            {
                "column": "cvegeo",
                "ref_column": "cvegeo",
                "ref_table": "census_2020_ageb",
            }
        ],
    },
)
def tree_coverage_2020_ageb(df_agebs: None) -> pd.DataFrame:
    bbox_global = get_all_agebs_bbox(df_agebs)
    img_coverage = filter_coverage_img(bbox_global)
    cvegeo_chunks = get_cvegeo_chunks(df_agebs)

    return concat_processed_chunks(
        cvegeo_chunks.map(
            lambda chunk: process_tree_coverage_chunk(chunk, img_coverage)
        ).collect()
    )
