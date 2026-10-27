from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

import pandas as pd


@dataclass(frozen=True)
class CensusSource:
    archive_template: str
    member_template: str


CENSUS_SOURCES = {
    2010: CensusSource(
        archive_template="resageburb_{state:02d}_2010_csv.zip",
        member_template=(
            "resultados_ageb_urbana_{state:02d}_cpv2010/conjunto_de_datos/"
            "resultados_ageb_urbana_{state:02d}_cpv2010.csv"
        ),
    ),
    2020: CensusSource(
        archive_template="ageb_mza_urbana_{state:02d}_cpv2020_csv.zip",
        member_template=(
            "ageb_mza_urbana_{state:02d}_cpv2020/conjunto_de_datos/"
            "conjunto_de_datos_ageb_urbana_{state:02d}_cpv2020.csv"
        ),
    ),
}


def read_census_archive(directory: Path, year: int, state: int) -> pd.DataFrame:
    """Decode one state archive without interpreting its statistical values."""
    specification = CENSUS_SOURCES[year]
    path = directory / specification.archive_template.format(state=state)
    member = specification.member_template.format(state=state)
    # Jalisco 2020 is the only Latin-1 CSV in the frozen archives.
    encoding = "latin1" if (year, state) == (2020, 14) else "utf-8-sig"
    with ZipFile(path) as archive, archive.open(member) as source:
        frame = pd.read_csv(
            source, encoding=encoding, dtype="string", keep_default_na=False
        )
    return frame.rename(columns=str.lower).assign(_source_file=path.name)


def load_census(directory: Path, year: int) -> pd.DataFrame:
    """Read all 32 states, aligning their published attributes by column name."""
    return pd.concat(
        [read_census_archive(directory, year, state) for state in range(1, 33)],
        ignore_index=True,
    )
