from collections.abc import Sequence

import pandas as pd
import requests

import dagster as dg


class PathResource(dg.ConfigurableResource):
    data_path: str


class LyraResource(dg.ConfigurableResource):
    host: str
    headers: dict[str, str]

    def request(self, endpoint: str, cvegeos: Sequence[str]) -> pd.Series:
        url = f"https://{self.host}/{endpoint}/cvegeo"
        response = requests.post(
            url, headers=self.headers, json={"cvegeo": list(cvegeos)}, timeout=100
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            err = f"Request to {url} failed with status code {response.status_code}: {response.text}"
            raise RuntimeError(err) from e

        return pd.Series(response.json())
