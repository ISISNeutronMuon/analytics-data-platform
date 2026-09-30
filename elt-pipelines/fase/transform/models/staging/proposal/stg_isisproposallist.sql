{{
    config(
        materialized = 'view'
    )
}}

with source as (

    select distinct
        rp.submission_date as "submission date",
        try_cast(rp.reference_number as int) as "rb number",
        coalesce(old_rbs.legacy_num, try_cast(rp.reference_number as int)) as "original rb number",
        rp.requested_equipment as "instrument requested",
        rp.requested_time as "requested time",
        rp.allocated_equipment as "instrument allocated",
        rp.allocated_time as "allocated time",
        pi.family_name || ' ' || pi.title || ' ' || coalesce(pi.given_name, pi.first_name_known_as) as "pi name",
        first_value(hp.org_country) over (partition by rp.reference_number, rp.pi_un order by hp.thru_date asc nulls last, hp.from_date asc) as "pi country",
        first_value(hp.org_name)    over (partition by rp.reference_number, rp.pi_un order by hp.thru_date asc nulls last, hp.from_date asc) as "pi organisation",
        first_value(hp.dept_name)   over (partition by rp.reference_number, rp.pi_un order by hp.thru_date asc nulls last, hp.from_date asc) as "pi department",
        rp.pi_un as "pi user number",
        pi.account_email as "pi email", -- business use only
        ec.family_name || ' ' || ec.title || ' ' || coalesce(ec.given_name, ec.first_name_known_as) as "ec name",
        ec.account_email as "ec email", -- business use only
        coalesce(contact_answer.first_value, legacy_contact.display_name) as "local contact",
        rp."route" as "access route",
        coalesce(panel.code, sp.fap) as "fap",
        '<a href="https://proposal.facilities.rl.ac.uk/?proposalid=' || rp.reference_number || '">proposal</a>' as "proposal",
        sub.abstract as "abstract",
        rp.title as "title",
        rp."round" as "round"
    from {{ ref('stg_reporting_proposal') }} rp
    left join {{ ref('stg_reporting_person') }} pi
        on rp.pi_un = pi.user_number
    left join {{ source('fase_proposal_registry', 'unique_role_assignment') }} ecrole
        on rp.proposal_id = ecrole.proposal_id
        and ecrole."role" = 'experiment-contact'
    left join {{ source('fase_proposal_registry', 'proposer') }} ecproposer
        on ecrole.proposer_id = ecproposer.proposer_id
    left join {{ ref('stg_reporting_person') }} ec
        on ecproposer.un = ec.user_number
    left join {{ source('fase_proposal', 'proposals') }} sub
        on rp.reference_number = sub.proposal_id
    left join {{ source('fase_proposal', 'fap_proposals') }} sub_panel
        on sub.proposal_pk = sub_panel.proposal_pk
    left join {{ source('fase_proposal', 'faps') }} panel
        on sub_panel.fap_id = panel.fap_id
    left join {{ ref('stg_proposal_answers') }} contact_answer
        on sub.proposal_pk = contact_answer.proposal_pk
        and contact_answer.question_natural_key = 'beamline_scientist'
    left join {{ source('fase_facility_schedule', 'sp_proposal_list_cache') }} sp
        on try_cast(rp.reference_number as int) = sp.rb_no
        and rp.facility = sp.facility_name
    left join {{ ref('stg_reporting_person') }} legacy_contact
        on sp.local_contact_un = legacy_contact.user_number
    left join {{ ref('stg_historical_person') }} hp
        on cast(rp.pi_un as varchar) = hp.user_number
        and (hp.thru_date is null or hp.thru_date > rp.submission_date)
    left join {{ source('fase_facility_common', 'legacy_reference_numbers') }} old_rbs
        on rp.reference_number = old_rbs.replacement
    where rp.facility = 'ISIS'
    and rp.withdrawn = 0
    order by try_cast(rp.reference_number as int) desc

),

cleaned as (

    select
        "submission date",
        "rb number",
        "original rb number",
        "instrument requested",
        "requested time",
        "instrument allocated",
        "allocated time",
        "pi name",
        "pi country",
        "pi organisation",
        "pi department",
        "pi user number",
        "pi email",
        "ec name",
        "ec email",
        "local contact",
        "access route",
        "fap",
        "proposal",
        "abstract",
        "title",
        "round"
    from source

)

select * from cleaned
