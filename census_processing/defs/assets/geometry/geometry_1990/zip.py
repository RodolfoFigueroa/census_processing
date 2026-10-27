"""Load authoritative 1990 AGEB geometries from the geometry ZIP."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import geopandas as gpd

if TYPE_CHECKING:
    import os


def load_geometry_zip(path: os.PathLike) -> gpd.GeoDataFrame:
    """Return ZIP geometries indexed by cvegeo in EPSG:6372, without repairs."""
    path = Path(path)

    member = "AGEBs 90_TecMonty_aj/AGEB's_90_aj.shp"
    frame = gpd.read_file(f"/vsizip/{path.resolve()}/{member}")
    frame.columns = frame.columns.str.lower()
    frame["cvegeo"] = (
        frame.cve_ent.astype(str).str.zfill(2)
        + frame.cve_mun.astype(str).str.zfill(3)
        + frame.cve_loc.astype(str).str.zfill(4)
        + frame.cve_ageb.astype(str).str.zfill(4)
    )
    return frame[["cvegeo", "geometry"]].set_index("cvegeo").to_crs(6372)
