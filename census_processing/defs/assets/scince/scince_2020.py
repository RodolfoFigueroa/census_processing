from census_processing.defs.assets.scince.common.common_2010_2020 import (
    scince_2010_2020_factory,
)
from census_processing.readers.scince import SCINCE_LEVELS

scince_2020_assets = [scince_2010_2020_factory(2020, level) for level in SCINCE_LEVELS]
