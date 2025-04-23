-- persjoner_pensjon

select
    navn,
    navident,
    team_navn,
    rolle,
    rolle2,
    ansettelsestype,
    po_navn,
    omrade_navn,
    -- omrade_type,
    startdato_nav,
    tilhorighet_niva
from {{ ref('personer_med_tilhorighet') }}
where po_navn = 'PO pensjon'