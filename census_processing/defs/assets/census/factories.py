import tempfile
import zipfile
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.defs.ops.extract import extract_op_map
from census_processing.defs.ops.geometry import merge_census_and_geometry
from census_processing.defs.ops.remove_unused import remove_unused_op_map
from census_processing.defs.resources import PathResource


def census_2010_2020_factory(
    *,
    year: int,
    zip_template: str,
    inner_dir_template: str,
    csv_template: str,
) -> dg.AssetsDefinition:
    @dg.asset(
        key=["staging", str(year), "census", "demographics"],
        ins={
            "demography_dep": dg.AssetIn(
                key=["input", str(year), "demography"], dagster_type=dg.Nothing
            )
        },
        io_manager_key="dataframe_file_manager",
        group_name=f"staging/census/{year}",
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

        return out.assign(
            cvegeo=lambda df: (
                df["entidad"].astype(int).astype(str).str.zfill(2)
                + df["mun"].astype(int).astype(str).str.zfill(3)
                + df["loc"].astype(int).astype(str).str.zfill(4)
                + df["ageb"].astype(str).str.zfill(4)
                + df["mza"].astype(str).str.zfill(3)
            ),
        ).drop(
            columns=[
                "entidad",
                "mun",
                "loc",
                "ageb",
                "mza",
            ],
            errors="ignore",
        )

    return _asset


def full_2010_2020_factory(
    level: Literal["ent", "mun", "loc", "ageb"],
    year: Literal[2010, 2020],
    *,
    geometry_op_map: dict[str, dg.OpDefinition],
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        key=["staging", str(year), "census", level],
        ins={
            "census": dg.AssetIn(key=["staging", str(year), "census", "demographics"]),
            "geometry_input": dg.AssetIn(
                key=["input", str(year), "geometry"], dagster_type=dg.Nothing
            ),
        },
        metadata={"table": f"census_{year}_{level}_prepared", "schema": "staging"},
        group_name=f"staging/census/{year}",
    )
    def _asset(
        census: pd.DataFrame, geometry_input: None
    ) -> dict[str, gpd.GeoDataFrame]:
        census = extract_op_map[level](census)
        census = remove_unused_op_map[level](census)

        geometry = geometry_op_map[level](geometry_input)

        return merge_census_and_geometry(census, geometry)

    return _asset
