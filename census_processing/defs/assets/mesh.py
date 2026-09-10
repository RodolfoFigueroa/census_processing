import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd

import dagster as dg
from census_processing.defs.resources import PathResource


def mesh_factory(level: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["staging", "mesh", f"level_{level}"],
        deps=[["input", "mesh"]],
        io_manager_key="postgres_manager",
        group_name="staging_mesh",
        metadata={"table": f"mesh_level_{level}_prepared", "schema": "staging"},
    )
    def _asset(path_resource: PathResource) -> gpd.GeoDataFrame:
        if level < 4 or level > 10:
            err = f"Invalid mesh level: {level}. Valid levels are between 4 and 10."
            raise ValueError(err)

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            zipfile.ZipFile(Path(path_resource.in_path) / "889463868842_gpk.zip") as zf,
        ):
            zf.extractall(tmpdir)
            tmpdir_path = Path(tmpdir)

            if level == 10:
                fpath = "malla_nivel10.gpkg"
                zip_path = "malla_nivel10_gpk.zip"
                layer = None
            else:
                fpath = "malla_niveles_4_al_9_continental_e_islas.gpkg"
                zip_path = "malla_niveles_4_al_9_continental_e_islas_gpk.zip"
                layer = f"n{level}"

            with (
                tempfile.TemporaryDirectory() as tmpdir2,
                zipfile.ZipFile(tmpdir_path / zip_path) as zf2,
            ):
                zf2.extractall(tmpdir2)
                tmpdir2_path = Path(tmpdir2)

                return gpd.read_file(
                    tmpdir2_path
                    / fpath.replace(".", "_")
                    / "conjunto_de_datos"
                    / fpath,
                    layer=layer,
                ).to_crs("EPSG:6372")

    return _asset


meshes = [mesh_factory(level) for level in range(4, 10)]
