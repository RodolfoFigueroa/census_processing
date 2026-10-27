import re
import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import NoReturn

from jinja2 import Environment, StrictUndefined, select_autoescape

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


def render_sql(template: str, target_schema: str = "public") -> str:
    """Render templates without a dbt runtime or database connection."""
    environment = Environment(
        undefined=StrictUndefined,
        extensions=["jinja2.ext.do"],
        autoescape=select_autoescape(enabled_extensions=(), default_for_string=False),
    )
    macros = "\n".join(
        path.read_text() for path in sorted((DBT / "macros").glob("*.sql"))
    )
    result = environment.from_string(macros + template).render(
        adapter=OfflineAdapter(),
        exceptions=CompilerErrors(),
        ref=OfflineAdapter.quote,
        source=source_relation,
        target=SimpleNamespace(schema=target_schema),
    )
    return " ".join(result.split())


def sqlite_sql(sql: str) -> str:
    """Adapt only the string operations used by the geographic macros."""
    sql = re.sub(r"left\(([\w.]+), (\d+)\)", r"substr(\1, 1, \2)", sql)
    sql = re.sub(r"right\(([\w.]+), (\d+)\)", r"substr(\1, -\2)", sql)
    sql = re.sub(r"substring\((.*?) from (\d+) for (\d+)\)", r"substr(\1, \2, \3)", sql)
    return sql.replace(" ~ ", " REGEXP ")


@contextmanager
def sqlite_database() -> Generator[sqlite3.Connection]:
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.create_function(
            "regexp",
            2,
            lambda pattern, value: (
                value is not None and bool(re.search(pattern, value))
            ),
        )
        connection.create_function("repeat", 2, lambda value, count: value * count)
        yield connection
