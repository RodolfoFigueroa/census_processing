import subprocess
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
    "disc": "discapacidad",
    "eco": "caracteristicas_economicas",
    "edu": "educacion",
    "fec": "fecundidad",
    "hogar": "hogares_censales",
    "indi": "etnicidad",
    "mig": "migracion",
    "mor": "mortalidad",
    "relig": "religion",
    "salud": "servicios_de_salud",
    "scony": "situacion_conyugal",
    "viv": "vivienda",
}


@dg.op(
    ins={
        "state_code": dg.In(dagster_type=str),
        "scince_file_dep": dg.In(dagster_type=dg.Nothing),
    }
)
def process_state_code_scince_2020(
    path_resource: PathResource, state_code: str
) -> dict[str, pd.DataFrame]:
    in_path = Path(path_resource.in_path)
    fpath = in_path / "2020" / "scince" / f"SCINCE2020_DATOS_{state_code}.zip"

    with (
        zipfile.ZipFile(fpath) as zf,
        tempfile.TemporaryDirectory() as tmpdir_1,
        tempfile.TemporaryDirectory() as tmpdir_2,
    ):
        zf.extractall(tmpdir_1)
        exe_path = next(Path(tmpdir_1).glob("**/*.exe"))
        subprocess.run(  # noqa: S603
            ["/usr/bin/innoextract", str(exe_path), "-d", tmpdir_2],
            check=True,
            stdout=subprocess.DEVNULL,
        )

        data_path = next((Path(tmpdir_2) / "app").glob(f"{state_code}*"))

        return read_extracted_tables(
            main_path=data_path / "cartografia" / "ageb_urb.shp",
            supplementary_table_template=str(
                data_path / "tablas" / "cpv2020_ageb_urb_{file_suffix}.dbf"
            ),
            supplementary_table_labels_map=SUPPLEMENTARY_TABLES_LABEL_MAP,
        )


scince_2020 = scince_2010_2020_factory(
    year=2020,
    process_op=process_state_code_scince_2020,
    table_suffixes=[*list(SUPPLEMENTARY_TABLES_LABEL_MAP.keys()), "pob"],
)
