from pathlib import Path

import ee
from dagster_components.managers import (
    DataFramePostgresManager,
    GeoDataFramePostGISManager,
)
from dagster_components.resources import PostGISResource

import dagster as dg
from census_processing.defs.managers import (
    DataFrameManager,
    GeoDataFrameManager,
)
from census_processing.defs.resources import PathResource

ee.Initialize()


@dg.definitions
def defs() -> dg.Definitions:
    main_defs = dg.load_from_defs_folder(project_root=Path(__file__).parent.parent)

    path_resource = PathResource(data_path=dg.EnvVar("DATA_PATH"))
    extra_defs = dg.Definitions(
        resources={
            "path_resource": path_resource,
            "postgis_resource": PostGISResource(
                host=dg.EnvVar("POSTGRES_HOST"),
                port=dg.EnvVar("POSTGRES_PORT"),
                db=dg.EnvVar("POSTGRES_DB"),
                user=dg.EnvVar("POSTGRES_USER"),
                password=dg.EnvVar("POSTGRES_PASSWORD"),
            ),
            "geodataframe_manager": GeoDataFrameManager(
                suffix=".gpkg",
                path_resource=path_resource,
            ),
            "dataframe_manager": DataFrameManager(
                suffix=".parquet",
                path_resource=path_resource,
            ),
            "dataframe_postgres_manager": DataFramePostgresManager(
                host=dg.EnvVar("POSTGRES_HOST"),
                port=dg.EnvVar("POSTGRES_PORT"),
                user=dg.EnvVar("POSTGRES_USER"),
                password=dg.EnvVar("POSTGRES_PASSWORD"),
                db=dg.EnvVar("POSTGRES_DB"),
            ),
            "geodataframe_postgis_manager": GeoDataFramePostGISManager(
                host=dg.EnvVar("POSTGRES_HOST"),
                port=dg.EnvVar("POSTGRES_PORT"),
                user=dg.EnvVar("POSTGRES_USER"),
                password=dg.EnvVar("POSTGRES_PASSWORD"),
                db=dg.EnvVar("POSTGRES_DB"),
            ),
        },
    )
    return dg.Definitions.merge(main_defs, extra_defs)
