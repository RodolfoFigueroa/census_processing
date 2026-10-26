import json
from pathlib import Path

from cfc_dagster_utils.managers.dataframe import DataFrameFileManager

import dagster as dg
from census_processing.defs.resources import PathResource, ScinceColumnsToKeepResource


@dg.definitions
def defs() -> dg.Definitions:
    root_path = Path(__file__).parent.parent
    data_path = root_path / "data"

    main_defs = dg.load_from_defs_folder(project_root=root_path)

    path_resource = PathResource(
        in_path=str(data_path / "input"),
        config_path=str(root_path / "config"),
    )

    scince_cols_to_keep_map: dict[str, list[str] | None] = {}
    for year in (1990, 2000, 2010, 2020):
        cols_to_keep_path = (
            root_path / "config" / "scince_cols_to_keep" / f"{year}.json"
        )
        if cols_to_keep_path.exists():
            with cols_to_keep_path.open("r") as f:
                scince_cols_to_keep_map[f"columns_{year}"] = list(json.load(f).keys())
        else:
            scince_cols_to_keep_map[f"columns_{year}"] = None

    extra_defs = dg.Definitions(
        executor=dg.multiprocess_executor.configured({"max_concurrent": 4}),
        resources={
            "dataframe_file_manager": DataFrameFileManager(
                path_resource=path_resource, extension=".parquet"
            ),
            "path_resource": path_resource,
            "scince_columns_to_keep_resource": ScinceColumnsToKeepResource(
                **scince_cols_to_keep_map
            ),
        },
    )
    return dg.Definitions.merge(main_defs, extra_defs)
