"""ZIP-first 1990 AGEB geometries with translated SCINCE fallbacks."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from pyproj.exceptions import ProjError
from shapely.affinity import translate
from shapely.errors import GEOSException
from shapely.geometry import MultiPolygon, Polygon

import dagster as dg
from census_processing.defs.assets.geometry.geometry_1990.scince import (
    TARGET_EPSG,
    Boundary,
    load_locality_boundaries,
    open_scince_maps,
    project_boundaries,
)
from census_processing.defs.assets.geometry.geometry_1990.zip import (
    load_geometry_zip,
)
from census_processing.defs.resources import PathResource  # noqa: TC001

UTM_ZONES = range(11, 17)
CANDIDATES = {"nad27": 26700, "itrf92": 4473}


@dataclass
class Candidate:
    source_epsg: int
    geometries: list[MultiPolygon]
    median_distance: float


def _usable(geometry: object) -> bool:
    return (
        isinstance(geometry, (Polygon, MultiPolygon))
        and not geometry.is_empty
        and geometry.is_valid
        and geometry.area > 0
        and math.isfinite(geometry.area)
        and bool(np.isfinite(shapely.get_coordinates(geometry)).all())
    )


def _choose_candidate(
    controls: list[Boundary],
    reference: gpd.GeoSeries,
) -> Candidate | None:
    best = None
    # Insertion order fixes ties: NAD27 before ITRF92, then ascending zone.
    for base in CANDIDATES.values():
        for zone in UTM_ZONES:
            epsg = base + zone
            try:
                geometries = project_boundaries(controls, epsg)
                distances = [
                    source.centroid.distance(target.centroid)
                    for source, target in zip(geometries, reference, strict=True)
                ]
                score = float(np.median(distances))
            except (ValueError, ProjError, GEOSException):
                continue
            if math.isfinite(score) and (best is None or score < best.median_distance):
                best = Candidate(epsg, geometries, score)
    return best


def _align(
    boundaries: list[Boundary],
    primary: gpd.GeoDataFrame,
) -> tuple[Candidate, tuple[float, float]] | None:
    controls = [
        boundary
        for boundary in boundaries
        if boundary.cvegeo in primary.index
        and _usable(primary.loc[boundary.cvegeo, "geometry"])
    ]
    if not controls:
        return None
    reference = primary.loc[[boundary.cvegeo for boundary in controls]].geometry
    candidate = _choose_candidate(controls, reference)
    if candidate is None:
        return None
    try:
        displacements = np.array(
            [
                [
                    target.centroid.x - source.centroid.x,
                    target.centroid.y - source.centroid.y,
                ]
                for source, target in zip(candidate.geometries, reference, strict=True)
            ]
        )
        dx, dy = (float(value) for value in np.median(displacements, axis=0))
    except (ValueError, GEOSException):
        return None
    if not all(math.isfinite(value) for value in (dx, dy)):
        return None
    return candidate, (dx, dy)


def _locality_fallbacks(
    boundaries: list[Boundary],
    ids: set[str],
    primary: gpd.GeoDataFrame,
) -> list[dict[str, object]]:
    missing = ids - set(primary.index)
    by_id = {boundary.cvegeo: boundary for boundary in boundaries}
    alignment = _align(boundaries, primary) if missing & by_id.keys() else None
    rows: list[dict[str, object]] = []
    for key in sorted(missing):
        geometry = None
        if key in by_id and alignment is not None:
            candidate, offset = alignment
            boundary = by_id[key]
            try:
                projected = project_boundaries(
                    [boundary],
                    candidate.source_epsg,
                )
                moved = translate(projected[0], xoff=offset[0], yoff=offset[1])
                if _usable(moved):
                    geometry = moved
            except (ValueError, ProjError, GEOSException):
                geometry = None
        rows.append({"cvegeo": key, "geometry": geometry})
    return rows


@dg.asset(
    key=["staging", "geometry", "1990", "ageb"],
    deps=[["input", "1990", "geometry", "ageb"], ["input", "1990", "scince"]],
    io_manager_key="postgres_manager",
    metadata={"schema": "staging", "table": "geometry_1990_ageb_prepared"},
    group_name="staging/1990/geometry",
)
def load_1990_ageb_geometries(path_resource: PathResource) -> gpd.GeoDataFrame:
    """Load ZIP geometries, aligned fallbacks, and unresolved statistical IDs.

    ZIP rows take unconditional precedence, including invalid geometries. SCINCE
    fallbacks are translated using shared AGEBs, without positional validation.

    The result contains only ``cvegeo`` and ``geometry`` in EPSG:6372. Unresolved
    statistical IDs retain null geometries.
    """

    year_path = Path(path_resource.in_path) / "1990"
    geometry_zip = year_path / "AGEBs 90_TecMonty_aj.zip"
    scince_rar = year_path / "SCINCE1990.rar"

    primary = load_geometry_zip(geometry_zip)
    rows: list[dict[str, object]] = []
    primary_ids = set(primary.index)

    with open_scince_maps(scince_rar) as maps:
        for path, ids in maps:
            missing_ids = ids - primary_ids
            if len(missing_ids) > 0:
                boundaries = load_locality_boundaries(path)
                rows.extend(_locality_fallbacks(boundaries, ids, primary))
    output = primary.reset_index()
    if rows:
        output = gpd.GeoDataFrame(
            pd.concat(
                [output, gpd.GeoDataFrame(rows, crs=TARGET_EPSG)], ignore_index=True
            ),
            geometry="geometry",
            crs=TARGET_EPSG,
        )
    return output.sort_values("cvegeo").reset_index(drop=True)
