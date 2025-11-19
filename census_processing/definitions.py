from pathlib import Path

import dagster as dg
from census_processing.defs.managers import DataFrameManager, GeoDataFrameManager
from census_processing.defs.resources import PathResource


@dg.definitions
def defs() -> dg.Definitions:
    main_defs = dg.load_from_defs_folder(project_root=Path(__file__).parent.parent)

    path_resource = PathResource(data_path=dg.EnvVar("DATA_PATH"))
    extra_defs = dg.Definitions(
        resources={
            "path_resource": path_resource,
            "geodataframe_manager": GeoDataFrameManager(
                suffix=".gpkg",
                path_resource=path_resource,
            ),
            "dataframe_manager": DataFrameManager(
                suffix=".parquet",
                path_resource=path_resource,
            ),
        },
    )
    return dg.Definitions.merge(main_defs, extra_defs)
