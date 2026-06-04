import dataclasses as dc
from pathlib import Path


@dc.dataclass(frozen=True)
class ELTJobManifest:
    """Parsed representation of an ELT job"""

    name: str
    domain: str
    ingest_job_dir: Path

    @property
    def full_name(self) -> str:
        return f"{self.domain}.{self.name}"

    @property
    def destination_namespace(self) -> str:
        """The destination namespace for this job: ``{domain}_{name}``."""
        return f"{self.domain}_{self.name}"
