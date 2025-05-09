-- aggregert_omrade_roller
-- aggregerer rolletyper i et område med kjønn og ansettelsestype
-- bruker for enkelthets skyld kun første rolle

with

ref_personer as (
    select
        navident,
        rolle,
        omrade_navn
    from {{ ref('personer_med_tilhorighet') }}
    -- from `pensjon-saksbehandli-prod-1f83.teamkatalogen_historikk.personer_med_tilhorighet`
),

ref_hr_data_utvalg as (
    select
        navident,
        kjonn,
        ansettelsestype
    from {{ source('teamkatalogen_historikk', 'hr_data_utvalg') }}
    -- from `pensjon-saksbehandli-prod-1f83.teamkatalogen_historikk.hr_data_utvalg`
),

ref_seed_roller as (
    select
        rollenavn as rolle,
        rolletype
    from {{ ref('seed_teamkatalogen_roller_dimensjon') }}
    -- from `pensjon-saksbehandli-prod-1f83.teamkatalogen_historikk.seed_teamkatalogen_roller_dimensjon`
),

person_med_rolletype as (
    select
        ref_personer.*,
        ref_seed_roller.rolletype
    from ref_personer
    left join ref_seed_roller
        on ref_personer.rolle = ref_seed_roller.rolle
),


unike_personer_med_hr_data as (
    select
        person_med_rolletype.navident,
        person_med_rolletype.omrade_navn,
        person_med_rolletype.rolletype,
        ref_hr_data_utvalg.kjonn,
        ref_hr_data_utvalg.ansettelsestype,
    from person_med_rolletype
    left join ref_hr_data_utvalg
        on person_med_rolletype.navident = ref_hr_data_utvalg.navident
),

aggregert as (
    select
        omrade_navn,
        rolletype,
        kjonn,
        ansettelsestype,
        count(distinct navident) as antall_personer
    from unike_personer_med_hr_data
    group by
        omrade_navn,
        rolletype,
        kjonn,
        ansettelsestype
),

final as (
    select
        omrade_navn,
        rolletype,
        kjonn,
        ansettelsestype,
        antall_personer
    from aggregert
)

select * from final
