-- aggregert_team_ansettelsestype

with

ref_personer as (
    select
        navident,
        team_navn,
        coalesce(po_navn, omrade_navn) as omrade_navn,
        tilhorighet_niva
    from {{ ref('personer_med_tilhorighet') }}
),

ref_hr_data_utvalg as (
    select
        navident,
        ansettelsestype
    from {{ source('teamkatalogen_historikk', 'hr_data_utvalg') }}
),

sammensmeltet as (
    select
        ref_personer.team_navn,
        ref_personer.omrade_navn,
        ref_hr_data_utvalg.ansettelsestype,
    from ref_personer
    left join ref_hr_data_utvalg
        on ref_personer.navident = ref_hr_data_utvalg.navident
),

aggregert as (
    select
        team_navn,
        omrade_navn,
        ansettelsestype,
        count(*) as antall_personer
    from sammensmeltet
    group by
        team_navn,
        omrade_navn,
        ansettelsestype
),

final as (
    select
        team_navn,
        omrade_navn,
        ansettelsestype,
        antall_personer
    from aggregert
)

select * from final
