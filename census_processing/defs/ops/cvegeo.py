import pandas as pd

import dagster as dg


@dg.op
def add_one_higher_level_cvegeo(df: pd.DataFrame) -> pd.DataFrame:
    cvegeo_length = df["cvegeo"].str.len().iloc[0]

    if cvegeo_length == 2:
        err = "cvegeo is already at the highest level (entidad)"
        raise ValueError(err)

    if cvegeo_length == 5:
        df = df.assign(cve_ent=lambda df: df["cvegeo"].str[:2])
    elif cvegeo_length == 9:
        df = df.assign(cve_mun=lambda df: df["cvegeo"].str[:5])
    elif cvegeo_length == 13:
        df = df.assign(cve_loc=lambda df: df["cvegeo"].str[:9])
    elif cvegeo_length == 16:
        df = df.assign(cve_ageb=lambda df: df["cvegeo"].str[:13])

    return df


@dg.op
def add_higher_levels_cvegeo(df: pd.DataFrame) -> pd.DataFrame:
    cvegeo_length = df["cvegeo"].str.len().iloc[0]

    if cvegeo_length == 2:
        err = "cvegeo is already at the highest level (entidad)"
        raise ValueError(err)

    df = df.assign(cve_ent=lambda df: df["cvegeo"].str[:2])

    if cvegeo_length >= 9:
        df = df.assign(cve_mun=lambda df: df["cvegeo"].str[:5])

    if cvegeo_length >= 13:
        df = df.assign(cve_loc=lambda df: df["cvegeo"].str[:9])

    if cvegeo_length == 16:
        df = df.assign(cve_ageb=lambda df: df["cvegeo"].str[:13])

    return df
