import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from cfc_dagster_utils.utils import cast_all_columns_to_numeric

import dagster as dg
from census_processing.defs.resources import PathResource


def census_2010_2020_factory(
    *,
    year: int,
    zip_template: str,
    inner_dir_template: str,
    csv_template: str,
) -> dg.AssetsDefinition:
    @dg.asset(
        key=["staging", str(year), "census"],
        deps=[["input", str(year), "demography"]],
        io_manager_key="postgres_manager",
        metadata={"schema": "staging", "table": f"census_{year}_prepared"},
        group_name=f"staging/{year}",
    )
    def _asset(path_resource: PathResource) -> pd.DataFrame:
        raw_path = Path(path_resource.in_path)

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
                    ).replace({"*": pd.NA, "N/D": pd.NA})
                )

        out = pd.concat(df_census, ignore_index=True)
        out.columns = out.columns.str.lower()

        return (
            out.assign(
                cvegeo=lambda df: (
                    df["entidad"].astype(int).astype(str).str.zfill(2)
                    + df["mun"].astype(int).astype(str).str.zfill(3)
                    + df["loc"].astype(int).astype(str).str.zfill(4)
                    + df["ageb"].astype(str).str.zfill(4)
                    + df["mza"].astype(str).str.zfill(3)
                ),
            )
            .drop(
                columns=[
                    "entidad",
                    "mun",
                    "loc",
                    "ageb",
                    "mza",
                ],
                errors="ignore",
            )
            .pipe(
                lambda df: cast_all_columns_to_numeric(
                    df,
                    ignore=["nom_ent", "nom_mun", "nom_loc", "cvegeo"],
                    prefer_integer=True,
                )
            )
        )

    return _asset


census_2010 = census_2010_2020_factory(
    year=2010,
    zip_template="resageburb_{i:02d}_2010_csv.zip",
    inner_dir_template="resultados_ageb_urbana_{i:02d}_cpv2010",
    csv_template="resultados_ageb_urbana_{i:02d}_cpv2010.csv",
)

census_2020 = census_2010_2020_factory(
    year=2020,
    zip_template="ageb_mza_urbana_{i:02d}_cpv2020_csv.zip",
    inner_dir_template="ageb_mza_urbana_{i:02d}_cpv2020",
    csv_template="conjunto_de_datos_ageb_urbana_{i:02d}_cpv2020.csv",
)


@dg.op
def add_cve_met_column(
    df: gpd.GeoDataFrame, df_metropoli: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    mun_to_met_map = df_metropoli.set_index("cvegeo")["cve_met"].to_dict()
    return df.assign(cve_met=df["cvegeo"].map(mun_to_met_map))
