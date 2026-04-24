from collections.abc import Iterator

import pandas as pd
from dagster_components.resources import PostGISResource

import dagster as dg
from census_processing.defs.assets.common import concat_dataframes
from census_processing.defs.resources import LyraResource


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


def process_cvegeo_chunk_factory(name: str) -> dg.OpDefinition:
    for infix in ["tree_coverage", "urbanized_area"]:
        if infix in name:
            endpoint = infix
            break

    @dg.op(name=name)
    def _op(
        lyra_resource: LyraResource,
        chunkl: list[str],
    ) -> pd.DataFrame:
        response = lyra_resource.request(
            endpoint=endpoint,
            cvegeos=chunkl,
        )
        return response.rename("area_m2").reset_index(name="cvegeo")

    return _op


def reduce_ee_image_factory(decorator_kwargs: dict) -> dg.AssetsDefinition:
    op_name = "_".join(decorator_kwargs["key"]) + "_chunk_processor"
    process_op = process_cvegeo_chunk_factory(op_name)

    @dg.graph_asset(**decorator_kwargs)
    def _asset(df_agebs: None) -> pd.DataFrame:
        cvegeo_chunks = get_cvegeo_chunks(df_agebs)

        return concat_dataframes(cvegeo_chunks.map(process_op).collect())

    return _asset
