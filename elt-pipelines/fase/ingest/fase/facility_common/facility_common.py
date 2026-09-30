import sys
from pathlib import Path

ingest_dir = Path(__file__).resolve().parents[2]
if str(ingest_dir) not in sys.path:
    sys.path.insert(0, str(ingest_dir))

from common.oracle import BaseOracleExtract as Extract
from common.oracle import PipelineOracleConfig

__all__ = ["Extract", "PipelineOracleConfig"]
