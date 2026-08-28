import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.providers.standard.operators.empty import EmptyOperator

def run_statusdisplay_ingest():
    from elt_common.cli import cli as elt_cli
    elt_cli(['run', '/opt/analytics-data-platform/elt-pipelines/facility_ops', '--step=ingest', 'accelerator.statusdisplay'],  standalone_mode=False)

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
