from pathlib import Path

from pydantic import field_validator

import dagster as dg


class PathResource(dg.ConfigurableResource):
    in_path: str
    out_path: str
    config_path: str

    @field_validator("in_path", "out_path", "config_path", mode="after")
    @classmethod
    def path_exists(cls, path: str) -> str:
        if not Path(path).exists():
            err = f"Path {path} does not exist"
            raise FileNotFoundError(err)
        return path
