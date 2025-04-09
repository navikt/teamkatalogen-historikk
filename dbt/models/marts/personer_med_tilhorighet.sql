-- personer_med_tilhorighet
-- pakker ut members på team og på område, joiner de sammen og legger til personinfo
-- per april 2025 er det 641 dobbelttellinger av personer
-- er typ 75k rader i tabellen

with

omrade as (
    select
        id as omrade_id,
        name as omrade_navn,
        areatype as omrade_type,
        case 
            when areatype = 'PRODUCT_AREA' then concat('PO ', substr(name, 15))
            else null
        end as po_navn,
        members as omrade_members -- json
    from {{ ref('staging_produktomraader') }}
    where status = 'ACTIVE'
),

team as (
    select
        id as team_id,
        name as team_navn,
        productareaid as omrade_id,
        members as team_members -- json
    from {{ ref('staging_teams') }}
    where status = 'ACTIVE'
),

person as (
    select
        navident,
        fullname as navn,
        startdate as startdato_nav,
        resourcetype as ansettelsestype
    from {{ ref('staging_personer') }}
),

team_personer as (
    select
        team.team_navn,
        omrade.omrade_navn,
        omrade.omrade_type,
        omrade.po_navn,
        json_value(member, '$.navIdent') as navident,
        lower(json_value(member, '$.roles[0]')) as rolle,
        lower(json_value(member, '$.roles[1]')) as rolle2,
        'Team' as nivaa
    from
        team,
        unnest(json_extract_array(team.team_members)) as member
    left join omrade
        on team.omrade_id = omrade.omrade_id
),

omrade_personer as (
    select
        null as team_navn,
        omrade.omrade_navn,
        omrade.omrade_type,
        omrade.po_navn,
        json_value(member, '$.navIdent') as navident,
        lower(json_value(member, '$.roles[0]')) as rolle,
        lower(json_value(member, '$.roles[1]')) as rolle2,
        'Område' as nivaa
    from
        omrade,
        unnest(json_extract_array(omrade.omrade_members)) as member
),

union_team_omrade as (
    select
        team_navn,
        omrade_navn,
        omrade_type,
        po_navn,
        navident,
        rolle,
        rolle2,
        nivaa
    from team_personer
    union all
    select
        null as team_navn,
        omrade_navn,
        omrade_type,
        po_navn,
        navident,
        rolle,
        rolle2,
        nivaa
    from omrade_personer
),

join_personinfo as (
    select
        person.navident,
        person.navn,
        person.startdato_nav,
        person.ansettelsestype,
        union_team_omrade.omrade_navn,
        union_team_omrade.team_navn,
        union_team_omrade.rolle,
        union_team_omrade.rolle2,
        union_team_omrade.omrade_type,
        union_team_omrade.po_navn,
        union_team_omrade.nivaa
    from person
    left join union_team_omrade
        on person.navident = union_team_omrade.navident
),



final as (
    select
        navn,
        navident,
        team_navn,
        rolle,
        rolle2,
        ansettelsestype,
        po_navn,
        omrade_navn,
        omrade_type,
        startdato_nav,
        nivaa
    from join_personinfo
)

select * from final
