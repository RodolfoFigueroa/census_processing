"""Load authoritative 1990 AGEB geometries from the geometry ZIP."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from zipfile import ZipFile

import geopandas as gpd

if TYPE_CHECKING:
    import os


def load_geometry_zip(path: os.PathLike) -> gpd.GeoDataFrame:
    """Return ZIP geometries indexed by cvegeo in EPSG:6372, without repairs."""
    path = Path(path)

    with ZipFile(path) as archive:
        members = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".shp") and not name.startswith("__MACOSX/")
        ]
    if len(members) != 1:
        msg = "Expected exactly one AGEB shapefile in the geometry ZIP"
        raise ValueError(msg)
    frame = gpd.read_file(f"/vsizip/{path.resolve()}/{members[0]}")
    frame.columns = frame.columns.str.lower()
    required = {"cve_ent", "cve_mun", "cve_loc", "cve_ageb", "geometry"}
    if not required.issubset(frame.columns) or frame.crs is None or frame.empty:
        msg = "Geometry ZIP must contain AGEB identifiers, geometries, and a CRS"
        raise ValueError(msg)
    frame["cvegeo"] = (
        frame.cve_ent.astype(str).str.zfill(2)
        + frame.cve_mun.astype(str).str.zfill(3)
        + frame.cve_loc.astype(str).str.zfill(4)
        + frame.cve_ageb.astype(str).str.zfill(4)
    )
    if not frame.cvegeo.str.fullmatch(r"[0-9]{12}[0-9A-Z]").all():
        msg = "Malformed AGEB identifiers in geometry ZIP"
        raise ValueError(msg)
    if frame.cvegeo.duplicated().any():
        msg = "Duplicate AGEB identifiers in geometry ZIP"
        raise ValueError(msg)
    return frame[["cvegeo", "geometry"]].set_index("cvegeo").to_crs(6372)
