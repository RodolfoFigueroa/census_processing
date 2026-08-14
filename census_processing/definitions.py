from pathlib import Path

from cfc_dagster_utils.managers.dataframe import DataFrameFileManager

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.definitions
def defs() -> dg.Definitions:
    root_path = Path(__file__).parent.parent
    data_path = root_path / "data"

    main_defs = dg.load_from_defs_folder(project_root=root_path)

    path_resource = PathResource(
        in_path=str(data_path / "input"),
        out_path=str(data_path / "output"),
        config_path=str(root_path / "config"),
    )

    extra_defs = dg.Definitions(
        resources={
            "dataframe_file_manager": DataFrameFileManager(
                path_resource=path_resource, extension=".parquet"
            ),
            "path_resource": path_resource,
        },
    )
    return dg.Definitions.merge(main_defs, extra_defs)
