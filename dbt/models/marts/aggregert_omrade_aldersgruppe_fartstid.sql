-- aggregert_omrade_aldersgruppe_fartstid
-- aggregerer aldersgruppe og fartstid per omrade

with

ref_personer as (
    select
        navident,
        date_diff(current_date, startdato_nav, DAY) / 365 as fartstid_nav_ar,
        omrade_navn
    from {{ ref('personer_med_tilhorighet') }}
    -- from `pensjon-saksbehandli-prod-1f83.teamkatalogen_historikk.personer_med_tilhorighet`
),

ref_hr_data_utvalg as (
    select
        navident,
        aldersgruppe
    from {{ source('teamkatalogen_historikk', 'hr_data_utvalg') }}
    -- from `pensjon-saksbehandli-prod-1f83.teamkatalogen_historikk.hr_data_utvalg`
),

unike_personer_med_hr_data as (
    select
        ref_personer.omrade_navn,
        round(avg(ref_personer.fartstid_nav_ar), 0) as snitt_fartstid_nav_ar,
        round(sum(ref_personer.fartstid_nav_ar), 0) as sum_fartstid_nav_ar,
        ref_hr_data_utvalg.aldersgruppe,
        count(distinct ref_personer.navident) as antall_personer
    from ref_personer
    left join ref_hr_data_utvalg
        on ref_personer.navident = ref_hr_data_utvalg.navident
    group by
        omrade_navn,
        aldersgruppe
),

final as (
    select
        omrade_navn,
        aldersgruppe,
        snitt_fartstid_nav_ar,
        sum_fartstid_nav_ar,
        antall_personer
    from unike_personer_med_hr_data
)

select * from final
