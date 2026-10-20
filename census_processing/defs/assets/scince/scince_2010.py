import tempfile
import zipfile
from pathlib import Path

import pandas as pd

import dagster as dg
from census_processing.defs.assets.scince.common.common_2010_2020 import (
    read_extracted_tables,
    scince_2010_2020_factory,
)
from census_processing.defs.resources import PathResource

SUPPLEMENTARY_TABLES_LABEL_MAP = {
    "des": "desarrollo_social",
    "eco": "caracteristicas_economicas",
    "edu": "caracteristicas_educativas",
    "disc": "discapacidad",
    "fec": "fecundidad",
    "hogar": "hogares_censales",
    "indi": "lengua_indigena",
    "mig": "migracion",
    "relig": "religion",
    "salud": "servicios_de_salud",
    "scony": "situacion_conyugal",
    "viv": "viviendas",
}


@dg.op(
    ins={
        "state_code": dg.In(dagster_type=str),
        "scince_file_dep": dg.In(dagster_type=dg.Nothing),
    }
)
def process_scince_ageb_2010(
    path_resource: PathResource, state_code: str
) -> dict[str, pd.DataFrame]:
    in_path = Path(path_resource.in_path)
    fpath = in_path / "2010" / "scince" / f"{state_code}_SCINCE_zip.zip"

    with zipfile.ZipFile(fpath) as zf, tempfile.TemporaryDirectory() as tmpdir:
        zf.extractall(tmpdir)
        data_path = Path(tmpdir) / state_code
        return read_extracted_tables(
            main_path=data_path / "ageb_urb.shp",
            supplementary_table_template=str(
                data_path / "tablas" / "cpv2010_ageb_urb_{file_suffix}.dbf"
            ),
            supplementary_table_labels_map=SUPPLEMENTARY_TABLES_LABEL_MAP,
        )


scince_2010_ageb_pop = scince_2010_2020_factory(
    year=2010,
    process_op=process_scince_ageb_2010,
    table_suffixes=[*list(SUPPLEMENTARY_TABLES_LABEL_MAP.keys()), "pob"],
)
