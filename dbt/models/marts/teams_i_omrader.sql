-- teams_i_omrader
-- kombinerer Produktomraader og Teams til en rad per team. team til område er 1:1
-- klynger ligger i en liste formatert som streng i Teams. team til klynge er 1:mange

with

omrade as (
    select
        id as omrade_id,
        name as omrade_navn,
        status as omrade_status,
        areatype as omrade_type,
        case 
            when areatype = 'PRODUCT_AREA' then concat('PO ', substr(name, 15))
            else null
        end as po_navn,
        case
            when areatype = 'PRODUCT_AREA' then 1
            else 0
        end as po_flagg
    from {{ ref('staging_produktomraader') }}
),

team as (
    select
        id as team_id,
        name as team_navn,
        status as team_status,
        teamtype as team_type,
        productareaid as omrade_id
    from {{ ref('staging_teams') }}
),

sammensmeltet as (
    select
        team.team_id,
        team.team_navn,
        team.team_status,
        team.team_type,
        team.omrade_id,
        omrade.omrade_navn,
        omrade.po_navn,
        omrade.po_flagg,
        omrade.omrade_status
    from team
    left join omrade
        on team.omrade_id = omrade.omrade_id
),

final as (
    select
        team_navn,
        omrade_navn,
        po_navn,
        po_flagg,
        team_type,
        team_status,
        omrade_status,
        team_id,
        omrade_id
    from sammensmeltet
)

select * from final
