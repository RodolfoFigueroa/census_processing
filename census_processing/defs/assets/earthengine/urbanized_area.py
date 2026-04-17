import dagster as dg
from census_processing.defs.assets.earthengine.common import (
    reduce_ee_image_factory,
)

asset = reduce_ee_image_factory(
    decorator_kwargs={
        "key": ["urbanized_area", "2020", "ageb"],
        "ins": {
            "df_agebs": dg.AssetIn(
                key=["census", "2020", "ageb"], dagster_type=dg.Nothing
            )
        },
        "group_name": "ageb_stats_2020",
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
