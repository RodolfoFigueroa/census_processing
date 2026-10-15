from __future__ import annotations

import shutil
import struct
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import numpy as np
import pytest
from shapely.affinity import translate
from shapely.geometry import MultiPolygon, Polygon, box

from census_processing.defs.assets.geometry.geometry_1990 import ageb_1990 as loader
from census_processing.defs.assets.geometry.geometry_1990 import scince_1990 as scince
from census_processing.defs.assets.geometry.geometry_1990.ageb_1990 import (
    load_1990_ageb_geometries,
)
from census_processing.defs.assets.geometry.geometry_1990.scince_1990 import (
    INDEX_RECORD,
    PNF_RECORD_BYTES,
    Boundary,
    decode_map,
    decode_part,
    project_boundaries,
    statistical_agebs,
)
from census_processing.defs.assets.geometry.geometry_1990.zip_1990 import (
    load_geometry_zip,
)

PREFIX = "010010001"


def pnf(*labels: str) -> bytes:
    return b"".join(
        b"\x05" + label.encode("ascii") + b"\x08" + b" " * (PNF_RECORD_BYTES - 7)
        for label in labels
    )


def boundary(key: str, polygon: Polygon) -> Boundary:
    return Boundary(key, MultiPolygon([polygon]))


def identity_projection(
    boundaries: list[Boundary],
    _source_epsg: int,
) -> list[MultiPolygon]:
    return [item.geometry for item in boundaries]


@dataclass
class Inputs:
    geometry_zip: Path
    scince_rar: Path
    map_path: Path
    extracted: list[Path]


@pytest.fixture
def inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Inputs:
    labels = ["001-0", "002-0", "003-0", "004-0", "005-A", "006-0", "008-0"]
    polygons = [
        box(500000 + i * 200, 2000000, 500100 + i * 200, 2000100)
        for i in range(len(labels))
    ]
    map_path = tmp_path / "source/SCINCE/10/10010001"
    map_path.parent.mkdir(parents=True)
    map_path.with_suffix(".PNF").write_bytes(pnf(*labels[:6], "007-0"))
    index = []
    drawing = []
    for position, (label, polygon) in enumerate(zip(labels, polygons, strict=True)):
        coords = np.array(polygon.exterior.coords).flatten()
        data = struct.pack("<2f", 4, 0) + coords.astype("<f4").tobytes()
        drawing.append(data.ljust(80, b"\0"))
        index.append(INDEX_RECORD.pack(5, label.encode(), position, 5, *polygon.bounds))
    map_path.with_suffix(".IDX").write_bytes(b"".join(index))
    map_path.with_suffix(".DIB").write_bytes(b"".join(drawing))

    # Four valid shared controls, an invalid shared ZIP polygon, and a ZIP-only ID.
    invalid = Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)])
    primary = gpd.GeoDataFrame(
        {
            "CVE_ENT": [1] * 6,
            "CVE_MUN": [1] * 6,
            "CVE_LOC": [1] * 6,
            "CVE_AGEB": ["0010", "0020", "0030", "0040", "0060", "0090"],
        },
        geometry=[translate(polygon, xoff=10, yoff=-15) for polygon in polygons[:4]]
        + [invalid, box(0, 0, 1, 1)],
        crs=6372,
    )
    shp_dir = tmp_path / "shapefile"
    shp_dir.mkdir()
    primary.to_file(shp_dir / "agebs.shp", index=False)
    geometry_zip = tmp_path / "geometry.zip"
    with ZipFile(geometry_zip, "w") as archive:
        for path in shp_dir.iterdir():
            archive.write(path, f"nested/{path.name}")
    scince_rar = tmp_path / "scince.rar"
    scince_rar.touch()
    extracted: list[Path] = []

    def extract(_source: Path, destination: Path) -> None:
        extracted.append(destination)
        shutil.copytree(tmp_path / "source/SCINCE", destination / "SCINCE")

    monkeypatch.setattr(scince, "extract_geometry_archive", extract)
    monkeypatch.setattr(loader, "project_boundaries", identity_projection)
    return Inputs(geometry_zip, scince_rar, map_path, extracted)


def test_zip_loader_is_independent_of_scince(inputs: Inputs) -> None:
    primary = load_geometry_zip(inputs.geometry_zip)
    assert primary.index.name == "cvegeo"
    assert primary.columns.tolist() == ["geometry"]
    assert len(primary) == 6
    assert primary.crs is not None
    assert primary.crs.to_epsg() == 6372
    assert not primary.loc[PREFIX + "0060"].geometry.is_valid
    assert inputs.extracted == []


def test_scince_loading_is_independent_of_zip(inputs: Inputs) -> None:
    with scince.open_scince_maps(inputs.scince_rar) as maps:
        assert len(maps) == 1
        path, ids = maps[0]
        assert path.exists()
        assert ids == {
            PREFIX + suffix
            for suffix in ("0010", "0020", "0030", "0040", "005A", "0060", "0070")
        }
        boundaries = scince.load_locality_boundaries(path)
        by_id = {item.cvegeo: item.geometry for item in boundaries}
        assert PREFIX + "0080" in by_id
        assert PREFIX + "0070" not in by_id
        assert by_id[PREFIX + "005A"].equals(box(500800, 2000000, 500900, 2000100))
    assert inputs.extracted
    assert all(not path.exists() for path in inputs.extracted)


def test_scince_context_cleans_up_on_consumer_failure(inputs: Inputs) -> None:
    msg = "Consumer failed"
    with (
        pytest.raises(RuntimeError, match="Consumer failed"),
        scince.open_scince_maps(inputs.scince_rar),
    ):
        raise RuntimeError(msg)
    assert inputs.extracted
    assert all(not path.exists() for path in inputs.extracted)


def test_loader_precedence_null_rows_without_audit(inputs: Inputs) -> None:
    primary = load_geometry_zip(inputs.geometry_zip)
    output = load_1990_ageb_geometries(inputs.geometry_zip, inputs.scince_rar)
    assert len(output) == 8
    assert output.columns.tolist() == ["cvegeo", "geometry"]
    assert output.crs is not None
    assert output.crs.to_epsg() == 6372
    assert output.cvegeo.is_unique
    assert output.cvegeo.is_monotonic_increasing
    rows = output.set_index("cvegeo")
    assert (
        rows.loc[primary.index].geometry.to_wkb().tolist()
        == primary.geometry.to_wkb().tolist()
    )
    assert not rows.loc[PREFIX + "0060"].geometry.is_valid
    assert PREFIX + "0090" in rows.index
    assert PREFIX + "0080" not in rows.index
    fallback = rows.loc[PREFIX + "005A"].geometry
    assert isinstance(fallback, MultiPolygon)
    assert fallback.equals(box(500810, 1999985, 500910, 2000085))
    assert rows.loc[PREFIX + "0070"].geometry is None
    assert "fallback_audit" not in output.attrs
    assert inputs.extracted
    assert all(not path.exists() for path in inputs.extracted)


def test_no_missing_ids_avoids_drawing_decoding(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs.map_path.with_suffix(".PNF").write_bytes(pnf("001-0", "006-0"))

    def unexpected(*_args: object) -> None:
        pytest.fail("Fully covered localities must not decode drawings")

    monkeypatch.setattr(scince, "decode_map", unexpected)
    output = load_1990_ageb_geometries(inputs.geometry_zip, inputs.scince_rar)
    assert len(output) == 6
    assert output.columns.tolist() == ["cvegeo", "geometry"]
    assert "fallback_audit" not in output.attrs


@pytest.mark.parametrize(
    "shifts",
    [
        [(1010, -1015)] * 4,
        [(10, -15), (610, -815), (-990, 585), (3010, 2985)],
    ],
    ids=["large_shift", "inconsistent_controls"],
)
def test_large_or_inconsistent_shifts_are_applied(
    inputs: Inputs,
    monkeypatch: pytest.MonkeyPatch,
    shifts: list[tuple[float, float]],
) -> None:
    primary = load_geometry_zip(inputs.geometry_zip)
    for number, (dx, dy) in enumerate(shifts, 1):
        key = PREFIX + f"{number:03d}0"
        original = primary.loc[key, "geometry"]
        assert isinstance(original, (Polygon, MultiPolygon))
        primary.loc[key, "geometry"] = translate(
            original,
            xoff=dx - 10,
            yoff=dy + 15,
        )
    monkeypatch.setattr(loader, "load_geometry_zip", lambda _path: primary)
    output = load_1990_ageb_geometries(inputs.geometry_zip, inputs.scince_rar)
    row = output.set_index("cvegeo").loc[PREFIX + "005A"]
    fallback = row.geometry
    assert isinstance(fallback, MultiPolygon)
    dx, dy = np.median(shifts, axis=0)
    assert fallback.equals(
        translate(box(500800, 2000000, 500900, 2000100), xoff=dx, yoff=dy)
    )
    assert "fallback_audit" not in output.attrs


@pytest.mark.parametrize("control_count", [0, 1])
def test_one_shared_control_is_enough_but_none_keeps_null(
    inputs: Inputs,
    monkeypatch: pytest.MonkeyPatch,
    control_count: int,
) -> None:
    primary = load_geometry_zip(inputs.geometry_zip)
    for number in range(control_count + 1, 5):
        primary.loc[PREFIX + f"{number:03d}0", "geometry"] = primary.loc[
            PREFIX + "0060", "geometry"
        ]
    monkeypatch.setattr(loader, "load_geometry_zip", lambda _path: primary)
    output = load_1990_ageb_geometries(inputs.geometry_zip, inputs.scince_rar)
    row = output.set_index("cvegeo").loc[PREFIX + "005A"]
    assert "fallback_audit" not in output.attrs
    if control_count:
        fallback = row.geometry
        assert isinstance(fallback, MultiPolygon)
        assert fallback.equals(box(500810, 1999985, 500910, 2000085))
    else:
        assert row.geometry is None


def test_projection_failure_keeps_null_geometry(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    def failing(
        boundaries: list[Boundary],
        epsg: int,
    ) -> list[MultiPolygon]:
        if len(boundaries) == 1:
            msg = "Unusable projected fallback"
            raise ValueError(msg)
        return identity_projection(boundaries, epsg)

    monkeypatch.setattr(loader, "project_boundaries", failing)
    output = load_1990_ageb_geometries(inputs.geometry_zip, inputs.scince_rar)
    assert output.set_index("cvegeo").loc[PREFIX + "005A"].geometry is None
    assert output.columns.tolist() == ["cvegeo", "geometry"]


def test_missing_drawing_keeps_null_geometry(inputs: Inputs) -> None:
    inputs.map_path.with_suffix(".DIB").unlink()
    output = load_1990_ageb_geometries(inputs.geometry_zip, inputs.scince_rar)
    assert output.geometry.isna().sum() == 2
    missing = output.loc[output.geometry.isna()]
    assert missing.cvegeo.tolist() == [PREFIX + "005A", PREFIX + "0070"]
    assert output.columns.tolist() == ["cvegeo", "geometry"]
    assert "fallback_audit" not in output.attrs


@pytest.mark.parametrize("data", [b"partial", pnf("001-0", "001-0"), b""])
def test_bad_statistics_fail_and_clean_up(inputs: Inputs, data: bytes) -> None:
    inputs.map_path.with_suffix(".PNF").write_bytes(data)
    with pytest.raises(ValueError, match=r"Truncated|Duplicate|Empty"):
        load_1990_ageb_geometries(inputs.geometry_zip, inputs.scince_rar)
    assert inputs.extracted
    assert all(not path.exists() for path in inputs.extracted)


def test_duplicate_locality_fails(inputs: Inputs) -> None:
    duplicate = inputs.map_path.parent.parent / "11"
    duplicate.mkdir()
    shutil.copyfile(inputs.map_path.with_suffix(".PNF"), duplicate / "10010001.PNF")
    with pytest.raises(ValueError, match="Duplicate SCINCE locality"):
        load_1990_ageb_geometries(inputs.geometry_zip, inputs.scince_rar)


def test_duplicate_zip_ids_fail(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    frame = gpd.GeoDataFrame(
        {
            "CVE_ENT": [1, 1],
            "CVE_MUN": [1, 1],
            "CVE_LOC": [1, 1],
            "CVE_AGEB": ["0010", "0010"],
        },
        geometry=[box(0, 0, 1, 1)] * 2,
        crs=6372,
    )
    monkeypatch.setattr(gpd, "read_file", lambda *_args: frame)
    with pytest.raises(ValueError, match="Duplicate AGEB identifiers"):
        load_1990_ageb_geometries(inputs.geometry_zip, inputs.scince_rar)


def test_missing_archive_fails(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_1990_ageb_geometries(tmp_path / "absent.zip", tmp_path / "absent.rar")


def test_candidate_selection_and_unavailable_transformations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controls = [boundary(PREFIX + "0010", box(0, 0, 100, 100))]
    reference = gpd.GeoSeries([box(10, 0, 110, 100)], crs=6372)

    def project(
        boundaries: list[Boundary],
        epsg: int,
    ) -> list[MultiPolygon]:
        if epsg == 26711:
            msg = "No supported transformation"
            raise ValueError(msg)
        shapes = identity_projection(boundaries, epsg)
        if epsg in (26714, 4487):
            shapes = [translate(shape, xoff=10) for shape in shapes]
        return shapes

    monkeypatch.setattr(loader, "project_boundaries", project)
    candidate = loader._choose_candidate(controls, reference)
    assert candidate is not None
    assert candidate.source_epsg == 26714

    def unavailable(*_args: object) -> None:
        msg = "No supported transformation"
        raise ValueError(msg)

    monkeypatch.setattr(loader, "project_boundaries", unavailable)
    candidate = loader._choose_candidate(controls, reference)
    assert candidate is None


def test_statistical_ids_keep_alphanumeric_suffix() -> None:
    assert statistical_agebs(pnf("000-0", "005-A"), PREFIX) == {PREFIX + "005A"}


def test_decode_and_project_return_only_geometries(inputs: Inputs) -> None:
    index = inputs.map_path.with_suffix(".IDX").read_bytes() + b"x"
    drawing = inputs.map_path.with_suffix(".DIB").read_bytes()
    boundaries = decode_map(index, drawing, PREFIX)
    assert [item.cvegeo for item in boundaries] == [
        PREFIX + suffix
        for suffix in ("0010", "0020", "0030", "0040", "005A", "0060", "0080")
    ]
    for position, item in enumerate(boundaries):
        assert set(vars(item)) == {"cvegeo", "geometry"}
        assert item.geometry.equals(
            box(500000 + position * 200, 2000000, 500100 + position * 200, 2000100)
        )
    projected = project_boundaries(boundaries, 26714)
    assert isinstance(projected, list)
    assert len(projected) == len(boundaries)
    assert all(
        isinstance(shape, MultiPolygon) and shape.is_valid for shape in projected
    )
    expected = gpd.GeoSeries([item.geometry for item in boundaries], crs=26714).to_crs(
        6372
    )
    assert all(
        shape.equals_exact(reference, tolerance=0.001)
        for shape, reference in zip(projected, expected, strict=True)
    )


@pytest.mark.parametrize(
    ("drawing", "count"),
    [
        (b"", 5),
        (struct.pack("<2f", 4, 0) + bytes(16), 2),
        (struct.pack("<2f", 4, 1) + bytes(24), 3),
        (struct.pack("<2f", 3, 0) + bytes(24), 3),
        (struct.pack("<8f", 4, 0, float("nan"), 0, 1, 0, 1, 1), 3),
        (struct.pack("<8f", 4, 0, 0, 0, 1, 0, 2, 0), 3),
    ],
)
def test_unusable_drawing_parts_are_skipped(drawing: bytes, count: int) -> None:
    assert decode_part(drawing, 0, count) == []


def test_decoding_still_normalizes_closes_repairs_and_merges() -> None:
    # Open, self-intersecting ring with signed X coordinates.
    points = np.array([[-1, 0], [-3, 2], [-1, 2], [-3, 0]], dtype="<f4")
    drawing = struct.pack("<2f", 4, 0) + points.tobytes()
    # Repeated parts and inaccurate IDX bounds must not change the polygon.
    record = INDEX_RECORD.pack(5, b"005-A", 0, 4, 0, 0, 0, 0)
    boundaries = decode_map(record * 2, drawing, PREFIX)
    assert len(boundaries) == 1
    geometry = boundaries[0].geometry
    assert geometry.is_valid
    assert geometry.bounds == (1, 0, 3, 2)
    assert geometry.area == 2
    assert len(geometry.geoms) == 2


def test_real_archives_integration() -> None:
    root = Path(__file__).resolve().parents[1] / "data/input/1990"
    geometry_zip, scince_rar = (
        root / "AGEBs 90_TecMonty_aj.zip",
        root / "SCINCE1990.rar",
    )
    if not geometry_zip.exists() or not scince_rar.exists():
        pytest.skip("Original geometry and SCINCE archives required")
    output = load_1990_ageb_geometries(geometry_zip, scince_rar)
    primary = load_geometry_zip(geometry_zip)
    assert len(output) == 24119
    assert output.columns.tolist() == ["cvegeo", "geometry"]
    assert output.cvegeo.is_unique
    assert output.geometry.isna().sum() == 11
    assert len(primary) == 24105
    rows = output.set_index("cvegeo")
    assert (
        rows.loc[primary.index].geometry.to_wkb().tolist()
        == primary.geometry.to_wkb().tolist()
    )
    assert (~rows.loc[primary.index].geometry.is_valid).sum() == 7
    fallbacks = output.loc[~output.cvegeo.isin(primary.index) & output.geometry.notna()]
    assert fallbacks.cvegeo.tolist() == [
        "1603600010042",
        "1900900010231",
        "2800300430928",
    ]
    assert rows.loc["2800300430928"].geometry.is_valid
    assert "1510600010970" in primary.index
    assert fallbacks.geometry.is_valid.all()
    assert "fallback_audit" not in output.attrs
