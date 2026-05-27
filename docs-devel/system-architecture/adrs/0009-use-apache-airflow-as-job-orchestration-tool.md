# 9. Use Apache Airflow as job orchestration tool

Date: 2026-05-27

## Status

Accepted

## Context

The Lakehouse data platform runs ingestion jobs to harvest data from source systems into
the lake. These jobs currently run using cron to schedule them and capture logs to files
on the same node. Monitoring jobs is difficult and not scalable as it requires logging into
the node and checking the most recent log file for each job. It is also not possible to have
notifications issued when jobs failed.

## Decision

Initially the options to be considered were:

- Airflow
- Dagster
- Prefect
- Kestra
- Luigi

and our requirements are simple:

- Mature with strong community
- Tasks to be defined as Python code/packages.
- Allows a cron-like timed schedule to be created.
- A REST API to trigger jobs.
- Allows one-off jobs triggered manually on request.
- A web-based UI.
- Container-based deployment.

[Apache Airflow](https://airflow.apache.org/) is already in use around teams across STFC and
there is interest in setting up a local community of practice.
Given that Airflow supports all of the above requirements this is the strongest driver to choose Airflow.

## Consequences

Jobs will have greater visibility with the ability to notify on failure and manual restart through
a simple GUI button. Job history can also be inspected more easily through the tool.

On the downside Airflow becomes another tool to maintain as part of the platform infrastructure and new developers
will need to understand it as they onboard into the project. As more DAG jobs are developed the
overhead of moving to another tool, if needed, will become burdensome.
