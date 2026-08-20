

import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.providers.standard.operators.empty import EmptyOperator

def run_statusdisplay_ingest():
    from pathlib import Path
    from elt_common.ingest import run_ingest
    from elt_common.pipeline_types import ELTIngestManifest

    import logging
    logger = logging.getLogger(__name__)
    logger.info("Running statusdisplay ingest job")
    job = ELTIngestManifest(
        warehouse_name="facility_ops",
        name="statusdisplay",
        domain="accelerator",
        job_dir=Path(
            "/opt/analytics-data-platform/elt-pipelines/facility_ops/ingest/accelerator/statusdisplay"
        ),
    )
    run_ingest(job)

@dag(
    dag_id="statusdisplay",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 28, tz="UTC"),
    catchup=False,
    tags=["statusdisplay"]
)
def statusdisplay_dag():
    extract = PythonVirtualenvOperator(
        task_id="extract_statusdisplay",
        python_callable=run_statusdisplay_ingest,
        system_site_packages=True,
        requirements=[
            "/opt/analytics-data-platform/elt-pipelines[statusdisplay]",
            "/opt/analytics-data-platform/elt-common",
        ],
    )

    transform = EmptyOperator(task_id="transform_statusdisplay")

    extract >> transform

dag = statusdisplay_dag()
