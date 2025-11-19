from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource


class BaseManager(dg.ConfigurableIOManager):
    suffix: str
    path_resource: dg.ResourceDependency[PathResource]

    def _get_path(
        self,
        context: dg.InputContext | dg.OutputContext,
        *,
        create_parent: bool,
    ) -> Path:
        out_path = (
            Path(self.path_resource.data_path)
            / "processed"
            / "/".join(context.asset_key.path)
        )
        out_path = out_path.with_suffix(self.suffix)
        if create_parent:
            out_path.parent.mkdir(parents=True, exist_ok=True)
        return out_path

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:  # noqa: ANN401
        raise NotImplementedError

    def load_input(self, context: dg.InputContext) -> Any:  # noqa: ANN401
        raise NotImplementedError


class DataFrameManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        fpath = self._get_path(context, create_parent=True)

        if self.suffix == ".parquet":
            obj.to_parquet(fpath, index=False)
        elif self.suffix == ".csv":
            obj.to_csv(fpath, index=False)
        else:
            err = f"Unsupported suffix for DataFrameManager: {self.suffix}"
            raise ValueError(err)

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        fpath = self._get_path(context, create_parent=False)

        if self.suffix == ".parquet":
            return pd.read_parquet(fpath)
        if self.suffix == ".csv":
            return pd.read_csv(fpath)
        err = f"Unsupported suffix for DataFrameManager: {self.suffix}"
        raise ValueError(err)


class GeoDataFrameManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: gpd.GeoDataFrame) -> None:
        fpath = self._get_path(context, create_parent=True)

        if self.suffix == ".parquet":
            obj.to_parquet(fpath)
        elif self.suffix == ".gpkg":
            obj.to_file(fpath)
        else:
            err = f"Unsupported suffix for GeoDataFrameManager: {self.suffix}"
            raise ValueError(err)

    def load_input(self, context: dg.InputContext) -> gpd.GeoDataFrame:
        fpath = self._get_path(context, create_parent=False)

        if self.suffix == ".parquet":
            return gpd.read_parquet(fpath)
        if self.suffix == ".gpkg":
            return gpd.read_file(fpath)

        err = f"Unsupported suffix for GeoDataFrameManager: {self.suffix}"
        raise ValueError(err)
