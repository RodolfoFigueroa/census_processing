from pathlib import Path

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.definitions
def defs() -> dg.Definitions:
    main_defs = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)

    path_resource = PathResource(data_path=str(Path(__file__).parent.parent / "data"))

    extra_defs = dg.Definitions(
        resources={
            "path_resource": path_resource,
        },
    )
    return dg.Definitions.merge(main_defs, extra_defs)
