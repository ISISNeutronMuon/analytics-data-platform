from pathlib import Path
from typing import List

# patch which providers to enable
from dlt.common.configuration.providers import (
    ConfigProvider,
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.runtime.run_context import RunContext


def initial_providers(self) -> List[ConfigProvider]:
    # do not read the global config
    thisdir = Path(__file__).parent
    return [
        EnvironProvider(),
        SecretsTomlProvider(settings_dir=f"{thisdir}/.dlt"),
        ConfigTomlProvider(settings_dir=f"{thisdir}/.dlt"),
    ]


RunContext.initial_providers = initial_providers  # type: ignore[method-assign]
