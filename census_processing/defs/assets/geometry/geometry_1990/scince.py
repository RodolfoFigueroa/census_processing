"""Load, decode, and project geometries from SCINCE 1990 archives."""

from __future__ import annotations

import re
import shutil
import struct
import subprocess
import tempfile
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import rarfile
import shapely
from pyproj.transformer import AreaOfInterest, TransformerGroup
from shapely.errors import GEOSException
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon

if TYPE_CHECKING:
    import os
    from collections.abc import Generator

    from numpy.typing import NDArray
    from shapely.geometry.base import BaseGeometry

INDEX_RECORD = struct.Struct("<B7sHH4f")
DRAWING_HEADER = struct.Struct("<2f")
DRAWING_BLOCK_BYTES = 80
PNF_RECORD_BYTES = 645
TARGET_EPSG = 6372
AGEB_LABEL = re.compile(r"[0-9]{3}-[0-9A-Z]")
LOCALITY_STEM = re.compile(r"[1-9A-W][0-9]{7}")


@dataclass
class Boundary:
    cvegeo: str
    geometry: MultiPolygon


def locality_code(stem: str) -> str:
    return f"{int(stem[0], 36):02d}{stem[1:]}"


def statistical_agebs(data: bytes, prefix: str) -> set[str]:
    """Read the fixed-width PNF keys, excluding the locality total."""
    keys = set()
    for offset in range(0, len(data), PNF_RECORD_BYTES):
        label = data[offset + 1 : offset + 6].decode("ascii")
        if label != "000-0":
            keys.add(prefix + label.replace("-", ""))
    return keys


def _statistical_maps(root: Path) -> list[tuple[Path, set[str]]]:
    paths = sorted(
        path
        for path in root.glob("[0-9A-Z][0-9]/*.PNF")
        if LOCALITY_STEM.fullmatch(path.stem)
    )
    return [
        (path, statistical_agebs(path.read_bytes(), locality_code(path.stem)))
        for path in paths
    ]


@contextmanager
def open_scince_maps(
    source: os.PathLike,
) -> Generator[list[tuple[Path, set[str]]], None, None]:
    """Expose locality paths and statistical IDs during temporary extraction.

    Drawing files are read separately with ``load_locality_boundaries``, allowing
    callers to skip unneeded localities. Paths are valid only inside the context.
    """
    with tempfile.TemporaryDirectory() as extracted:
        extract_geometry_archive(source, Path(extracted))
        yield _statistical_maps(Path(extracted) / "SCINCE")


def load_locality_boundaries(path: Path) -> list[Boundary]:
    """Read a locality's IDX/DIB geometry in its unverified source coordinates.

    ``path`` identifies the locality PNF file. Missing or unreadable drawings
    yield no boundaries; projection remains separate from decoding.
    """
    prefix = locality_code(path.stem)
    try:
        return decode_map(
            path.with_suffix(".IDX").read_bytes(),
            path.with_suffix(".DIB").read_bytes(),
            prefix,
        )
    except OSError:
        return []


def polygon_components(geometry: BaseGeometry) -> list[Polygon]:
    if geometry.is_empty:
        return []
    if isinstance(geometry, Polygon):
        return [geometry] if geometry.area > 0 else []
    if isinstance(geometry, (MultiPolygon, GeometryCollection)):
        return [part for child in geometry.geoms for part in polygon_components(child)]
    return []


def decode_part(
    drawing: bytes,
    block: int,
    count: int,
) -> list[Polygon]:
    """Decode positive-area polygon parts, repairing invalid rings when possible."""
    start = block * DRAWING_BLOCK_BYTES
    end = start + DRAWING_HEADER.size + count * 8
    if end > len(drawing):
        return []
    kind, reserved = DRAWING_HEADER.unpack_from(drawing, start)
    if kind != 4 or reserved != 0 or count < 3:
        return []
    points = (
        np.frombuffer(
            drawing,
            dtype="<f4",
            count=count * 2,
            offset=start + DRAWING_HEADER.size,
        )
        .reshape(-1, 2)
        .astype(np.float64)
    )
    points[:, 0] = np.abs(points[:, 0])
    if not np.isfinite(points).all():
        return []
    polygon = Polygon(points)
    try:
        return polygon_components(
            shapely.make_valid(polygon) if not polygon.is_valid else polygon,
        )
    except GEOSException:
        return []


def merge_polygons(parts: list[Polygon]) -> MultiPolygon:
    merged = shapely.union_all(parts)
    merged = shapely.make_valid(merged) if not merged.is_valid else merged
    geometry = MultiPolygon(polygon_components(merged))
    if geometry.is_empty or not geometry.is_valid:
        msg = "Merged geometry is empty or invalid"
        raise ValueError(msg)
    return geometry


def _decode_map_parts(
    index: bytes,
    drawing: bytes,
    prefix: str,
) -> dict[str, list[Polygon]]:
    """Collect polygon parts from complete IDX records."""
    grouped: dict[str, list[Polygon]] = defaultdict(list)
    complete_length = len(index) // INDEX_RECORD.size * INDEX_RECORD.size
    for length, raw, block, count, *_bounds in INDEX_RECORD.iter_unpack(
        index[:complete_length],
    ):
        label = raw[:length].decode("ascii", errors="replace")
        if length != 5 or not AGEB_LABEL.fullmatch(label) or label == "000-0":
            continue
        key = prefix + label.replace("-", "")
        parts = decode_part(drawing, block, count)
        if parts:
            grouped[key].extend(parts)
    return grouped


def decode_map(
    index: bytes,
    drawing: bytes,
    prefix: str,
) -> list[Boundary]:
    """Merge usable AGEB parts into boundaries keyed by geographic ID."""
    grouped = _decode_map_parts(index, drawing, prefix)
    boundaries = []
    for key, parts in sorted(grouped.items()):
        try:
            geometry = merge_polygons(parts)
        except (GEOSException, ValueError):
            continue
        boundaries.append(Boundary(key, geometry))
    return boundaries


def extract_geometry_archive(source: os.PathLike, destination: os.PathLike) -> None:
    """Prefer a single process over rarfile's subprocess-per-member extraction."""
    source = Path(source)
    destination = Path(destination)

    executable = shutil.which("unrar")
    if executable is not None:
        # The executable is resolved from PATH; filenames are separate arguments.
        subprocess.run(  # noqa: S603
            [executable, "x", "-idq", "-o-", str(source.resolve()), f"{destination}/"],
            check=True,
        )
    else:
        with rarfile.RarFile(source) as archive:
            archive.extractall(destination)


def project_boundaries(
    boundaries: list[Boundary],
    source_epsg: int,
) -> list[MultiPolygon]:
    """Project usable polygons without ballpark transformations."""
    group = TransformerGroup(
        source_epsg,
        TARGET_EPSG,
        always_xy=True,
        allow_ballpark=False,
        area_of_interest=AreaOfInterest(-118.5, 14, -86, 33),
    )
    if not group.transformers:
        msg = f"No non-ballpark transformation available for EPSG:{source_epsg}"
        raise ValueError(msg)
    transformer = group.transformers[0]

    def transform_coordinates(coordinates: NDArray[np.float64]) -> NDArray[np.float64]:
        return np.column_stack(
            transformer.transform(coordinates[:, 0], coordinates[:, 1]),
        )

    transformed = shapely.transform(
        [boundary.geometry for boundary in boundaries],
        transform_coordinates,
    )
    if not np.isfinite(shapely.get_coordinates(transformed)).all():
        msg = f"Nonfinite transformed coordinates for EPSG:{source_epsg}"
        raise ValueError(msg)
    geometries = []
    for boundary, geometry in zip(boundaries, transformed, strict=True):
        repaired = not geometry.is_valid
        fixed = shapely.make_valid(geometry) if repaired else geometry
        polygon = MultiPolygon(polygon_components(fixed))
        if polygon.is_empty or not polygon.is_valid:
            msg = (
                f"Unusable projected geometry for {boundary.cvegeo} "
                f"in EPSG:{source_epsg}"
            )
            raise ValueError(msg)
        geometries.append(polygon)
    return geometries
