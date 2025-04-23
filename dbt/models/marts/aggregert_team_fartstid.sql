-- aggregert_team_kjonn

with

ref_personer as (
    select
        team_navn,
        startdato_nav,
        coalesce(po_navn, omrade_navn) as omrade_navn,
        tilhorighet_niva
    from {{ ref('personer_med_tilhorighet') }}
),

beregne_snitt_fartstid as (
    select
        team_navn,
        omrade_navn,
        tilhorighet_niva,
        round(avg(date_diff(current_date, startdato_nav, DAY)) / 365, 1)  as snitt_fartstid_nav_ar,
        round(sum(date_diff(current_date, startdato_nav, DAY)) / 365, 1) as sum_fartstid_nav_ar
    from ref_personer
    group by
        team_navn,
        omrade_navn,
        tilhorighet_niva
),

final as (
    select
        team_navn,
        omrade_navn,
        tilhorighet_niva,
        snitt_fartstid_nav_ar,
        sum_fartstid_nav_ar,
    from beregne_snitt_fartstid
)

select * from final
