import tempfile
import zipfile
from pathlib import Path

import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource


def full_census_2010_2020_factory(
    *,
    year: int,
    zip_template: str,
    inner_dir_template: str,
    csv_template: str,
) -> dg.OpDefinition:
    @dg.op(name=f"census_{year}", ins={"demography": dg.In(dagster_type=dg.Nothing)})
    def _op(path_resource: PathResource) -> pd.DataFrame:
        raw_path = Path(path_resource.data_path) / "input"

        df_census: list[pd.DataFrame] = []

        for i in range(1, 33):
            compressed_path = raw_path / str(year) / "census" / zip_template.format(i=i)

            with (
                tempfile.TemporaryDirectory() as tmpdir,
                zipfile.ZipFile(compressed_path) as zf,
            ):
                zf.extractall(tmpdir)

                try:
                    temp = pd.read_csv(
                        Path(tmpdir)
                        / inner_dir_template.format(i=i)
                        / "conjunto_de_datos"
                        / csv_template.format(i=i),
                    )
                except UnicodeDecodeError:
                    temp = pd.read_csv(
                        Path(tmpdir)
                        / inner_dir_template.format(i=i)
                        / "conjunto_de_datos"
                        / csv_template.format(i=i),
                        encoding="latin1",
                    )
                df_census.append(
                    temp.rename(
                        columns={"ï»¿ENTIDAD": "ENTIDAD", 'ï»¿"entidad"': "entidad"},
                        errors="ignore",
                    )
                )

        out = pd.concat(df_census, ignore_index=True)
        out.columns = out.columns.str.lower()

        return out.assign(
            entidad=lambda df: df["entidad"].astype(int).astype(str).str.zfill(2),
            mun=lambda df: df["mun"].astype(int).astype(str).str.zfill(3),
            loc=lambda df: df["loc"].astype(int).astype(str).str.zfill(4),
            ageb=lambda df: df["ageb"].astype(str).str.zfill(4),
            mza=lambda df: df["mza"].astype(str).str.zfill(3),
            cvegeo=lambda df: (
                df["entidad"] + df["mun"] + df["loc"] + df["ageb"] + df["mza"]
            ),
        )

    return _op
