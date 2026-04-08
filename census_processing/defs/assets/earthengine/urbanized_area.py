import ee

import dagster as dg
from census_processing.defs.assets.earthengine.common import (
    reduce_ee_image_factory,
)


@dg.op
def load_urbanized_area_img(bbox: ee.Geometry) -> ee.Image:
    return (
        ee.ImageCollection("JRC/GHSL/P2023A/GHS_BUILT_S")
        .select("built_surface")
        .filterBounds(bbox)
        .mean()
    )


asset = reduce_ee_image_factory(
    reducer=ee.Reducer.sum(),
    scale=100,
    table_name="census_2020_ageb",
    img_op=load_urbanized_area_img,
    decorator_kwargs={
        "key": ["urbanized_area", "2020", "ageb"],
        "ins": {
            "df_agebs": dg.AssetIn(
                key=["census", "2020", "ageb"], dagster_type=dg.Nothing
            )
        },
        "group_name": "urbanized_area_2020",
        "metadata": {
            "table_name": "urbanized_area_2020_ageb",
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
