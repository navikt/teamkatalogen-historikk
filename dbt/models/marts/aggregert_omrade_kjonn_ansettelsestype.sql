-- aggregert_omrade_kjonn_ansettelsestype
-- aggregerer kjonn og ansettelsestype per omrade

with

ref_personer as (
    select
        navident,
        coalesce(po_navn, omrade_navn) as omrade_navn
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

unike_personer_med_hr_data as (
    select
        ref_personer.omrade_navn,
        ref_hr_data_utvalg.kjonn,
        ref_hr_data_utvalg.ansettelsestype,
        count(distinct ref_personer.navident) as antall_personer
    from ref_personer
    left join ref_hr_data_utvalg
        on ref_personer.navident = ref_hr_data_utvalg.navident
    group by
        omrade_navn,
        kjonn,
        ansettelsestype
),

final as (
    select
        omrade_navn,
        kjonn,
        ansettelsestype,
        antall_personer
    from unike_personer_med_hr_data
)

select * from final
