from pathlib import Path

from cfc_dagster_utils.managers.postgres import (
    DataFramePostgresManager,
    GeoDataFramePostGISManager,
)
from cfc_dagster_utils.resources import PostgresResource

import dagster as dg
from census_processing.defs.managers import (
    DataFrameManager,
    GeoDataFrameManager,
)
from census_processing.defs.resources import PathResource


@dg.definitions
def defs() -> dg.Definitions:
    main_defs = dg.load_from_defs_folder(project_root=Path(__file__).parent.parent)

    path_resource = PathResource(data_path=str(Path(__file__).parent.parent / "data"))

    postgres_resource = PostgresResource(
        host=dg.EnvVar("POSTGRES_HOST"),
        port=dg.EnvVar("POSTGRES_PORT"),
        db=dg.EnvVar("POSTGRES_DB"),
        user=dg.EnvVar("POSTGRES_USER"),
        password=dg.EnvVar("POSTGRES_PASSWORD"),
    )

    extra_defs = dg.Definitions(
        resources={
            "path_resource": path_resource,
            "postgres_resource": postgres_resource,
            "geodataframe_manager": GeoDataFrameManager(
                suffix=".gpkg",
                path_resource=path_resource,
            ),
            "dataframe_manager": DataFrameManager(
                suffix=".parquet",
                path_resource=path_resource,
            ),
            "dataframe_postgres_manager": DataFramePostgresManager(
                postgres_resource=postgres_resource, if_exists="cascade_replace"
            ),
            "geodataframe_postgis_manager": GeoDataFramePostGISManager(
                postgres_resource=postgres_resource, if_exists="cascade_replace"
            ),
        },
    )
    return dg.Definitions.merge(main_defs, extra_defs)
