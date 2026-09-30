{{
    config(
        materialized = 'view'
    )
}}


with source as (

    select
        p.user_number,
        p.from_date,
        p.thru_date,
        p.rid as person_rid,
        p.status,
        p.new_establishment_id as establishment_id,
        p.establishment_id as old_establishment_id,
        coalesce(e.establishment_name, eo.org_name) as org_name,
        coalesce(e.establishment_url, eo.org_url) as org_url,
        coalesce(e.country_name, a.country) as org_country,
        (
            select listagg(c.category_name, ', ') within group (order by c.category_name)
            from {{ source('fase_isisuserdb', 'establishment_category_link') }} ecl
            join {{ source('fase_isisuserdb', 'category') }} c on c.id = ecl.category_id
            where ecl.establishment_id = e.id
        ) as org_type,
        coalesce(d.department_name, eo.dept_name) as dept_name,
        (
            select listagg(l.label_name, ', ') within group (order by l.label_name)
            from {{ source('fase_isisuserdb', 'department_label_link') }} dll
            join {{ source('fase_isisuserdb', 'label') }} l on l.id = dll.label_id
            where dll.department_id = p.department_id
        ) as department_labels
    from {{ source('fase_isisuserdb', 'person') }} p
    left join {{ source('fase_isisuserdb', 'establishment_new') }} e
        on p.new_establishment_id = e.id
    left join {{ source('fase_isisuserdb', 'department') }} d
        on d.id = p.department_id
    left join {{ source('fase_isisuserdb', 'establishment') }} eo
        on p.establishment_id = eo.establishment_id
    left join {{ source('fase_isisuserdb', 'establishment') }} eo_later
        on eo_later.establishment_id = eo.establishment_id
       and eo_later.rid > eo.rid
    left join {{ source('fase_isisuserdb', 'address') }} a
        on eo.postal_address_id = a.postal_address_id
    left join {{ source('fase_isisuserdb', 'address') }} a_later
        on a_later.postal_address_id = a.postal_address_id
       and a_later.rid > a.rid
    where p.user_number is not null
      and eo_later.rid is null
      and a_later.rid is null

),

cleaned as (

    select
        user_number,
        from_date,
        thru_date,
        person_rid,
        status,
        establishment_id,
        old_establishment_id,
        org_name,
        org_url,
        org_country,
        org_type,
        dept_name,
        department_labels
    from source

)

select * from cleaned
