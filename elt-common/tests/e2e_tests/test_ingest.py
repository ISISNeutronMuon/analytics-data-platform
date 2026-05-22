"""Run an end-to-end test using the SQL Iceberg catalog"""

import datetime as dt
import json
from pathlib import Path

from click.testing import CliRunner
from elt_common.cli import cli
from pyiceberg.catalog import Catalog
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection
from pyiceberg.transforms import BucketTransform
from pyiceberg.partitioning import PartitionField, PartitionSpec


TEST_DIR = Path(__file__).parent
TEST_PIPELINE_ROOT = TEST_DIR / "isis-facility_ops"


def test_e2e_cli_ingest(catalog: Catalog):
    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(TEST_PIPELINE_ROOT), "domain_a.fake_source"])

    assert result.exit_code == 0
    comments_table_id = ("domain_a__fake_source", "comments")
    assert catalog.table_exists(comments_table_id)
    table = catalog.load_table(comments_table_id)

    # properties
    assert table.properties["ingest.watermark"] == json.dumps({"column": "id", "value": 2099})
    dur_since_last_update_at = dt.datetime.now(dt.UTC) - dt.datetime.fromisoformat(
        table.properties["ingest.last_updated_at"]
    )
    # this should have happened within seconds of now...
    assert dur_since_last_update_at.days == 0.0
    assert dur_since_last_update_at.seconds < 10.0

    # metadata
    assert table.sort_order() == SortOrder(SortField(1, direction=SortDirection.ASC))
    assert table.metadata.partition_specs == [
        PartitionSpec(
            PartitionField(
                field_id=1000, source_id=1, name="id_bucket", transform=BucketTransform(2)
            )
        )
    ]

    # data
    arrow_table = table.scan().to_arrow()
    assert arrow_table.column_names == ["id", "comment"]
    # both chunks should have been read
    assert arrow_table.num_rows == 2000
