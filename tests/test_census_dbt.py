import ast
import re
from functools import cache
from pathlib import Path
from types import SimpleNamespace
from typing import NoReturn

import pytest
import yaml
from jinja2 import Environment, StrictUndefined, select_autoescape

import dagster as dg
from census_processing.defs.assets.census.census_2010_2020 import (
    census_2010,
    census_2020,
)

DBT = Path(__file__).resolve().parents[1] / "dbt"


class OfflineAdapter:
    @staticmethod
    def quote(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def __getattr__(self, name: str) -> NoReturn:
        msg = f"Database adapter access is forbidden in unit tests: {name}"
        raise AssertionError(msg)


class CompilerErrors:
    @staticmethod
    def raise_compiler_error(message: str) -> NoReturn:
        raise ValueError(message)


def source_relation(source_name: str, table: str) -> str:
    schema = {
        "census_ingest": "ingest",
        "scince_ingest": "ingest",
        "census_staging": "staging",
    }[source_name]
    return f'"{schema}"."{table}"'


@cache
def dbt_variables() -> dict:
    return yaml.safe_load((DBT / "vars.yml").read_text())["vars"]


def render_sql(
    template: str, target_schema: str = "public", *, compact: bool = True
) -> str:
    """Render SQL text only; no dbt runtime, asset execution, or connections."""
    environment = Environment(
        undefined=StrictUndefined,
        extensions=["jinja2.ext.do"],
        autoescape=select_autoescape(enabled_extensions=(), default_for_string=False),
    )
    variables = dbt_variables()
    macros = "\n".join(
        path.read_text() for path in sorted((DBT / "macros").glob("*.sql"))
    )
    result = environment.from_string(macros + template).render(
        var=variables.__getitem__,
        adapter=OfflineAdapter(),
        exceptions=CompilerErrors(),
        ref=OfflineAdapter.quote,
        source=source_relation,
        target=SimpleNamespace(schema=target_schema),
    )
    return " ".join(result.split()) if compact else result.strip()


@pytest.mark.parametrize(("year", "count"), [(2010, 191), (2020, 223)])
@pytest.mark.parametrize(
    ("level", "width"), [("ent", 2), ("mun", 5), ("loc", 9), ("ageb", 13)]
)
def test_public_projection_matches_its_existing_contract(
    year: int, count: int, level: str, width: int
) -> None:
    directory = DBT / "models" / "census" / str(year) / level
    properties = yaml.safe_load((directory / "schema.yml").read_text())["models"][0]
    columns = ast.literal_eval(render_sql(properties["columns"]))
    expected = [column["name"] for column in columns]
    assert len(expected) == count + (level != "ageb")
    sql = render_sql((directory / f"census_{year}_{level}.sql").read_text())
    projection, relation = sql.split(" from ")
    assert projection.startswith(f"select left(cvegeo, {width}) as cvegeo")
    assert ["cvegeo", *re.findall(r'"([^"]+)"', projection)] == expected
    assert relation == f"\"census_{year}\" where census_level = '{level}'"
    assert "*" not in projection


@pytest.mark.parametrize("year", [2010, 2020])
def test_staging_projects_declared_types_and_provenance(year: int) -> None:
    variables = yaml.safe_load((DBT / "vars.yml").read_text())["vars"]
    columns = variables[f"census_{year}_columns"]
    sql = render_sql(
        (DBT / "models" / "staging" / "census" / f"census_{year}.sql").read_text()
    )
    assert f'from "ingest"."census_{year}"' in sql
    assert "lpad(ageb, 4, '0')" in sql
    assert "_source_file" in sql
    for column in columns:
        name, data_type = column["name"], column["data_type"]
        if name == "cvegeo":
            continue
        sql_type = "integer" if data_type == "int" else "double precision"
        assert f'end as "{name}"' in sql
        assert (
            f'"{name}"::numeric::{sql_type}' in sql
            if data_type == "int"
            else f'"{name}"::{sql_type}' in sql
        )
    for name in ("nom_ent", "nom_mun", "nom_loc"):
        assert f'else "{name}"::text end as {name}' in sql


def test_fractional_counts_are_guarded_before_integer_conversion() -> None:
    sql = render_sql('{{ census_typed_value("value", "int") }}')
    assert (
        "when value::numeric = trunc(value::numeric) then value::numeric::integer"
        in sql
    )
    assert "else ('Non-integral census count: ' || value)::integer" in sql


def test_null_policy_preserves_previous_missing_tokens() -> None:
    tokens = yaml.safe_load((DBT / "vars.yml").read_text())["vars"][
        "census_null_tokens"
    ]
    assert set(tokens) == {
        "",
        "#N/A",
        "#N/A N/A",
        "#NA",
        "-1.#IND",
        "-1.#QNAN",
        "-NaN",
        "-nan",
        "1.#IND",
        "1.#QNAN",
        "<NA>",
        "N/A",
        "NA",
        "NULL",
        "NaN",
        "None",
        "n/a",
        "nan",
        "null",
        "*",
        "N/D",
    }
    assert "0" not in tokens
    for data_type in ("text", "int", "float"):
        sql = render_sql('{{ census_typed_value("value", "' + data_type + '") }}')
        null_branch = sql.split("then null")[0]
        for token in tokens:
            assert f"'{token}'" in null_branch


def test_classification_emits_existing_suffix_rules_in_precedence_order() -> None:
    sql = render_sql('{{ census_level("cvegeo") }}')
    assert sql == (
        "case when cvegeo = repeat('0', 16) then 'national' "
        "when right(cvegeo, 14) = repeat('0', 14) then 'ent' "
        "when right(cvegeo, 11) = repeat('0', 11) then 'mun' "
        "when right(cvegeo, 7) = repeat('0', 7) then 'loc' "
        "when right(cvegeo, 3) = repeat('0', 3) then 'ageb' else 'mza' end"
    )


@pytest.mark.parametrize(
    ("target", "custom", "expected"),
    [
        ("public", "none", "public"),
        ("public", "'staging'", "staging"),
        ("alice", "none", "alice"),
        ("alice", "'staging'", "staging"),
        ("alice", "' staging '", "staging"),
    ],
)
def test_schema_generation(target: str, custom: str, expected: str) -> None:
    assert (
        render_sql("{{ generate_schema_name(" + custom + ", none) }}", target)
        == expected
    )


@pytest.mark.parametrize("year", [2010, 2020])
def test_declarations_link_ingestion_staging_and_geography(year: int) -> None:
    asset = {2010: census_2010, 2020: census_2020}[year]
    key = dg.AssetKey(["ingest", str(year), "census"])
    assert asset.keys == {key}
    assert asset.group_names_by_key[key] == f"ingest/{year}/census"
    assert asset.asset_deps[key] == {dg.AssetKey(["input", str(year), "demography"])}
    assert asset.get_io_manager_key_for_asset_key(key) == "postgres_manager"
    assert asset.metadata_by_key[key] == {"schema": "ingest", "table": f"census_{year}"}

    directory = DBT / "models" / "staging" / "census"
    source = yaml.safe_load((directory / "sources.yml").read_text())["sources"][0]
    table = next(
        table for table in source["tables"] if table["name"] == f"census_{year}"
    )
    assert source["schema"] == "ingest"
    assert table["config"]["meta"]["dagster"]["asset_key"] == list(key.path)
    assert table["config"]["meta"]["dagster"]["group"] == asset.group_names_by_key[key]
    models = yaml.safe_load((directory / "schema.yml").read_text())["models"]
    model = next(model for model in models if model["name"] == f"census_{year}")
    assert model["config"]["contract"]["enforced"] is True
    assert model["config"]["materialized"] == "table"
    assert model["config"]["schema"] == "staging"
    assert model["config"]["meta"]["dagster"]["asset_key"] == [
        "staging",
        str(year),
        "census",
    ]
    assert model["config"]["meta"]["dagster"]["group"] == f"staging/{year}/census"
    columns = ast.literal_eval(render_sql(model["columns"]))
    assert [column["name"] for column in columns][-5:] == [
        "nom_ent",
        "nom_mun",
        "nom_loc",
        "_source_file",
        "census_level",
    ]

    for level in ("ent", "mun", "loc", "ageb"):
        sql = render_sql("{{ geo_unit_ids(" + str(year) + ', "' + level + '") }}')
        assert f'from "census_{year}"' in sql
        assert f"census_{year}_prepared" not in sql
    assert f"census_{year}_prepared" not in (DBT / "models" / "sources.yml").read_text()


@pytest.mark.parametrize(
    "template",
    [
        '{{ census_typed_value("value", "boolean") }}',
        '{{ census_columns(2020, 13, "unknown_name") }}',
    ],
)
def test_unsupported_macro_arguments_fail(template: str) -> None:
    with pytest.raises(ValueError, match=r"Unsupported census type|keep_name must"):
        render_sql(template)
