with source as (

select * from {{ source('beamlines_archive_files', 'archive_files') }}

),

renamed as (

select

  file_name,
  replace(instrument_dir, 'NDX', '') as instrument,
  regexp_extract(file_name, '^(.*?)(\d{8}|\d{5}).+$', 2) as run_number,
  file_bytes,
  substring(file_name, strpos(file_name, '.', -1)) as file_ext,
  case
    when lower(file_name) like '%.nxs' then 'nxs'
    when lower(file_name) like '%.n%' then 'part_nxs'
    when lower(file_name) like '%.raw' then 'raw'
    when lower(file_name) like '%.s%' then 'raw'
    else 'other'
  end as file_type,
  instrument_dir,
  cycle_dir

from source

)

select * from renamed
