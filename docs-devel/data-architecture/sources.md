# Data Sources

The proof-of-concept will ingest data from the following sources:

- Crew logs: Opralog DB
- Vista/EPICS control system 2s data: InfluxDB
- Running schedule: ISIS status display [API](https://status.isis.stfc.ac.uk/api/schedule)

## Opralog

The crew logs are captured by Opralog and stored in a Microsoft SQL Server database.
Full backups of the database run every 24 hours and T-Logs are run hourly.

For the purposes of testing a full backup of the database was obtained from the
Infrastructure team in ISIS and copied to a [Manila share](https://openstack.stfc.ac.uk/project/shares/faae6094-4856-4322-bbc1-678e414e32dd/)
under the path `[mount_point]/staging/opralog/full-backups`

The [MSSQL documentation](https://learn.microsoft.com/en-us/sql/linux/tutorial-restore-backup-in-sql-server-container?view=sql-server-2017&tabs=cli)
describes how to restore this to a server instance running in a Linux container.
The following commands will restore the database to a new container with the data stored in a docker volume `sql1data`:

```sh
> docker run -e 'ACCEPT_EULA=Y' -e 'MSSQL_SA_PASSWORD=Strong@Passw0rd' \
  --name 'sql1' -p 1433:1433 \
  -v sql1data:/var/opt/mssql -v /mnt/isis_lakehouse/staging:/staging:ro \
  -d mcr.microsoft.com/mssql/server:2019-latest

> docker exec -it sql1 /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P 'Strong@Passw0rd' \
  -Q 'RESTORE DATABASE LOGBOOK FROM DISK = "/staging/opralog/full-backups/LOGBOOK_backup_2024_06_24_183655_2079164.bak" WITH MOVE "LOGBOOK_Data" TO "/var/opt/mssql/LOGBOOK_Data.mdf", MOVE "LOGBOOK_Log" TO "/var/opt/mssql/LOGBOOK_Log.ldf"'
```

## Control system 2s data

The 2s control system data is stored in an InfluxDB instance managed by the accelerator controls team.

For the purposes of testing a full backup of the database was obtained from the accelerator
controls team copied to a [Manila share](https://openstack.stfc.ac.uk/project/shares/faae6094-4856-4322-bbc1-678e414e32dd/)
under the path `[mount_point]/staging/influxdb/influxdbv2/data`

## Running schedule

The running schedule is available from the system supporting the physical
status display screens around ISIS buildings. A JSON document with the schedule
can be retrieved from <https://status.isis.stfc.ac.uk/api/schedule>.

It currently contains dates from 2019/1 onwards.
