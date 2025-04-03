with

po as (
    select 
        concat('PO ', substr(name, 15)) as po_navn,
        id as po_id
    from teamkatalogen_historikk.staging_produktomraader
    where status = 'ACTIVE'
    and areatype = 'PRODUCT_AREA'
),

-- har duplikater innad i PO der det er en person er i flere team
team_i_po as (
    select 
        name as team,
        -- productareaid as po_id,
        po.po_navn as po_navn,
        json_extract_scalar(member, '$.navIdent') as nav_id,
        lower(json_extract_scalar(member, '$.roles[0]')) as rolle,
        lower(json_extract_scalar(member, '$.roles[1]')) as rolle2
    from
        teamkatalogen_historikk.staging_teams,
        unnest(json_extract_array(staging_teams.members)) as member
        inner join po on staging_teams.productareaid = po.po_id
        where staging_teams.status = 'ACTIVE'
),

-- én rad per person i PO. Velger første rolle, som er litt tilfeldig
unike_identer_i_po as (
    select
        nav_id,
        po_navn,
        rolle
    from (
        select
            nav_id,
            po_navn,
            rolle,
            row_number() over (partition by nav_id order by po_navn, rolle) as rn
        from team_i_po
        )
    where rn = 1
),

po_pensjon as (
    select
        nav_id,
        team,
        po_navn,
        rolle,
        rolle2
    from team_i_po
    where po_navn = 'PO pensjon'
)

select * from po_pensjon
