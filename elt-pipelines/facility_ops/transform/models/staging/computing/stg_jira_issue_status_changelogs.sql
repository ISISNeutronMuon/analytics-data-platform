with source as (
select * from facility_ops_landing.computing_jira.issue_status_changelog
),

cleaned as (
select
	issue_key as issue_key,
	TRIM(LOWER(from_status)) as from_status,
	TRIM(LOWER(to_status)) as to_status,
	changed_at as changed_at
from source
)

select * from cleaned
