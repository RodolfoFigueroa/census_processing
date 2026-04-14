import ee

import dagster as dg
from census_processing.defs.assets.earthengine.common import (
    reduce_ee_image_factory,
)


@dg.op
def load_tree_coverage_img(bbox: ee.Geometry) -> ee.Image:
    return (
        ee.ImageCollection(
            "projects/sat-io/open-datasets/facebook/meta-canopy-height",
        )
        .filterBounds(bbox)
        .mean()
        .gte(ee.Number(3))
        .multiply(ee.image.Image.pixelArea())
    )


asset = reduce_ee_image_factory(
    reducer=ee.Reducer.sum(),
    scale=25,
    table_name="census_2020_ageb",
    img_op=load_tree_coverage_img,
    decorator_kwargs={
        "key": ["tree_coverage", "2020", "ageb"],
        "ins": {
            "df_agebs": dg.AssetIn(
                key=["census", "2020", "ageb"], dagster_type=dg.Nothing
            )
        },
        "group_name": "ageb_stats_2020",
        "metadata": {
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
    },
)
