import pytest

from tests.sql_helpers import render_sql, sqlite_database, sqlite_sql


@pytest.mark.parametrize(
    ("level", "expected"),
    [
        ("ent", {"01", "02", "03", "04", "05", "06", "07", "08", "09"}),
        (
            "mun",
            {"01001", "02002", "03003", "04004", "05005", "06006", "07007", "09009"},
        ),
        (
            "loc",
            {
                "010010001",
                "020020002",
                "030030003",
                "040040004",
                "050050005",
                "060060006",
            },
        ),
        ("ageb", {"010010001001A", "020020002002B", "030030003003C"}),
        ("mza", {"010010001001A001", "020020002002B002"}),
    ],
)
def test_identifier_union_preserves_source_only_units_and_ancestors(
    level: str, expected: set[str]
) -> None:
    sql = render_sql(
        "{{ geo_unit_ids_from_sources('" + level + "', "
        "{'loc': 'geometry'}, 'census', "
        "{'mza': 'blocks', 'ageb': 'agebs', 'loc': 'rural', "
        "'mun': 'municipal', 'ent': 'state'}, 'metro') }}"
    )
    sources = {
        "census": [
            "010010001001A001",
            "010010001001A001",
            "0400400040000000",
            "0000000000000000",
        ],
        "blocks": ["020020002002B002", "020020002002B002", "9909900990000000"],
        "agebs": ["030030003003C", "9909900990000"],
        "rural": ["050050005"],
        "geometry": ["060060006"],
        "municipal": ["07007"],
        "state": ["08"],
        "metro": ["09009"],
    }
    with sqlite_database() as conn:
        conn.execute("create table fixtures (source text, cvegeo text)")
        conn.executemany(
            "insert into fixtures values (?, ?)",
            [(name, key) for name, keys in sources.items() for key in keys],
        )
        ctes = """with
            census as (select cvegeo from fixtures where source = 'census'),
            blocks as (select cvegeo from fixtures where source = 'blocks'),
            agebs as (select cvegeo from fixtures where source = 'agebs'),
            rural as (select cvegeo from fixtures where source = 'rural'),
            geometry as (select cvegeo from fixtures where source = 'geometry'),
            municipal as (select cvegeo from fixtures where source = 'municipal'),
            state as (select cvegeo from fixtures where source = 'state'),
            metro as (select cvegeo from fixtures where source = 'metro')
        """
        actual = [r[0] for r in conn.execute(ctes + sqlite_sql(sql))]
    assert len(actual) == len(set(actual))
    assert set(actual) == expected


@pytest.mark.parametrize(
    ("level", "valid", "invalid"),
    [
        ("ent", "01", ["00"]),
        ("mun", "01001", ["01000", "00001"]),
        ("loc", "010010001", ["010010000", "010000001"]),
        ("ageb", "010010001001A", ["0100100010000"]),
        ("mza", "010010001001A001", ["010010001001A000", "0100100010000001"]),
    ],
)
def test_public_geography_filter_excludes_summary_identifiers(
    level: str, valid: str, invalid: list[str]
) -> None:
    template = 'select cvegeo from units where {{ valid_geo_unit("cvegeo", "LEVEL") }}'
    query = render_sql(template.replace("LEVEL", level))
    with sqlite_database() as conn:
        conn.execute("create table units (cvegeo text)")
        conn.executemany(
            "insert into units values (?)", [(key,) for key in [valid, *invalid]]
        )
        result = conn.execute(sqlite_sql(query)).fetchall()
    assert result == [(valid,)]


@pytest.mark.parametrize(
    ("level", "identifier", "parent"),
    [
        ("mza", "030178293912A001", "030178293912A"),
        ("ageb", "030178293912A", "030178293"),
        ("loc", "030178293", "03017"),
        ("mun", "03017", "03"),
        ("ent", "03", None),
    ],
)
def test_parent_projection_preserves_rows_and_payload(
    level: str, identifier: str, parent: str | None
) -> None:
    query = render_sql("{{ add_parent_cvegeo('units', '" + level + "') }}")
    rows = [(identifier, 7), (identifier, 7), (None, 11)]
    with sqlite_database() as connection:
        connection.execute("create table units (cvegeo text, payload integer)")
        connection.executemany("insert into units values (?, ?)", rows)
        result = connection.execute(sqlite_sql(query)).fetchall()
    expected = (
        rows if parent is None else [(identifier, 7, parent)] * 2 + [(None, 11, None)]
    )
    assert result == expected


def test_census_classification_distinguishes_totals_from_blocks() -> None:
    rows = [
        ("0000000000000000", "national"),
        ("0100000000000000", "ent"),
        ("0100100000000000", "mun"),
        ("0100100010000000", "loc"),
        ("010010001001A000", "ageb"),
        ("010010001001A001", "mza"),
    ]
    query = render_sql("select cvegeo, {{ census_level('cvegeo') }} from units")
    with sqlite_database() as connection:
        connection.execute("create table units (cvegeo text)")
        connection.executemany(
            "insert into units values (?)", [(key,) for key, _ in rows]
        )
        result = connection.execute(sqlite_sql(query)).fetchall()
    assert result == rows
