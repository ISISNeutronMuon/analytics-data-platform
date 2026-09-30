{{
    config(
        materialized = 'view'
    )
}}

with source as (

    select
        prop.proposal_pk,
        prop.proposal_id,
        gt.generic_template_id,
        template_q.natural_key as generic_template_natural_key,
        gt.title as generic_template_title,
        a.answer_id,
        q.natural_key as question_natural_key,
        json_format(json_extract(a.answer, '$.value')) as full_value,
        coalesce(
            json_extract_scalar(a.answer, '$.value[0]'),
            json_extract_scalar(a.answer, '$.value.value'),
            json_extract_scalar(a.answer, '$.value')
        ) as first_value
    from {{ source('fase_proposal', 'proposals') }} prop
    join {{ source('fase_proposal', 'generic_templates') }} gt
        on prop.proposal_pk = gt.proposal_pk
    join {{ source('fase_proposal', 'questions') }} template_q
        on cast(gt.question_id as varchar) = cast(template_q.question_id as varchar)
    join {{ source('fase_proposal', 'answers') }} a
        on gt.questionary_id = a.questionary_id
    join {{ source('fase_proposal', 'questions') }} q
        on cast(a.question_id as varchar) = cast(q.question_id as varchar)

    union

    select
        prop.proposal_pk,
        prop.proposal_id,
        cast(null as integer) as generic_template_id,
        cast(null as varchar) as generic_template_natural_key,
        cast(null as varchar) as generic_template_title,
        a.answer_id,
        q.natural_key as question_natural_key,
        json_format(json_extract(a.answer, '$.value')) as full_value,
        coalesce(
            json_extract_scalar(a.answer, '$.value[0]'),
            json_extract_scalar(a.answer, '$.value.value'),
            json_extract_scalar(a.answer, '$.value')
        ) as first_value
    from {{ source('fase_proposal', 'proposals') }} prop
    join {{ source('fase_proposal', 'answers') }} a
        on prop.questionary_id = a.questionary_id
    join {{ source('fase_proposal', 'questions') }} q
        on cast(a.question_id as varchar) = cast(q.question_id as varchar)

),

cleaned as (

    select
        proposal_pk,
        proposal_id,
        generic_template_id,
        generic_template_natural_key,
        generic_template_title,
        answer_id,
        question_natural_key,
        full_value,
        first_value
    from source

)

select * from cleaned
