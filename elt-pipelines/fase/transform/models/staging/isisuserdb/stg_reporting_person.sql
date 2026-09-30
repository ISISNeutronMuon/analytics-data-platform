{{
    config(
        materialized = 'view'
    )
}}

with source as (

    select
        p.user_number,
        p.family_name || ' ' || p.title || ' ' || coalesce(p.given_name, p.first_name_known_as) as display_name,
        p.title,
        p.given_name,
        p.first_name_known_as,
        p.family_name,
        case
            when p.deactivated = 1 then 'Deactivated'
            when p.marketing_subscription = 0 then 'Unsubscribed'
            when p.sha2 is null then 'Not Activated'
            else p.email
        end as marketing_email,
        case
            when p.deactivated = 1 then 'Deactivated'
            when p.sha2 is null then 'Not Activated'
            else p.email
        end as account_email,
        p.work_phone,
        p.mobile_phone,
        p.visiting_department,
        p.status,
        e.establishment_name as org_name,
        e.establishment_url as org_url,
        e.country_name as org_country,
        (
            select listagg(c.category_name, ', ') within group (order by c.category_name)
            from {{ source('fase_isisuserdb', 'establishment_category_link') }} ecl
            join {{ source('fase_isisuserdb', 'category') }} c on c.id = ecl.category_id
            where ecl.establishment_id = e.id
        ) as org_type,
        d.department_name as dept_name,
        (
            select listagg(l.label_name, ', ') within group (order by l.label_name)
            from {{ source('fase_isisuserdb', 'department_label_link') }} dll
            join {{ source('fase_isisuserdb', 'label') }} l on l.id = dll.label_id
            where dll.department_id = p.department_id
        ) as department_labels
    from {{ source('fase_isisuserdb', 'person') }} p
    left join {{ source('fase_isisuserdb', 'establishment_new') }} e
        on e.thru_date is null
       and p.new_establishment_id = e.id
    left join {{ source('fase_isisuserdb', 'department') }} d
        on d.id = p.department_id
    where p.thru_date is null
      and p.user_number is not null

),

cleaned as (

    select
        user_number,
        display_name,
        title,
        given_name,
        first_name_known_as,
        family_name,
        marketing_email,
        account_email,
        work_phone,
        mobile_phone,
        visiting_department,
        status,
        org_name,
        org_url,
        org_country,
        org_type,
        dept_name,
        department_labels
    from source

)

select * from cleaned
