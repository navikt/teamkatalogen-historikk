-- etterlevelse_bdok_med_alle_e_nr
-- unike B-nr med relevant info fra Behandlingskatalogen, samt
-- joinet inn alle koblede E-nr og team som er tagget på disse E-ene

with

teams_i_omrader as (
    -- henter team-id for å joine inn teamnavn
    select
        team_navn,
        omrade_navn as team_omrade_navn,
        team_id
    from `pensjon-saksbehandli-prod-1f83.teamkatalogen_historikk.teams_i_omrader`
),

bdok as (
    select
        concat('B', number) as b_nr,
        number as nr,
        `affiliation_seksjoner`[safe_offset(0)].nomSeksjonName as b_omrade,
        affiliation_nomDepartmentName as b_avdeling,
        name as b_navn,
        status as b_status,
        purpose_shortName as b_tema,
        coalesce(teams_i_omrader.team_navn, 'B-nr uten team') as b_team_navn,
        coalesce(teams_i_omrader.team_omrade_navn, 'B-nr uten team-område') as b_team_omrade_navn,
        string_agg(json_extract_scalar(products, '$.shortName'), ', ') over (partition by id) as relaterte_systemer,
        case
            when dpia_needForDpia = true then 'Ja'
            when dpia_needForDpia = false then 'Nei'
            else 'Ikke vurdert/satt'
        end as b_behovForPVK,
        case
            when dpia_needForDpia = true and dpia_refToDpia is not null then dpia_refToDpia
            when dpia_needForDpia = true and dpia_refToDpia is null then '(Trenger PVK, mangler referanse)'
            when dpia_needForDpia = false then '(PVK ikke relevant)'
            else '(Ukjent behov for PVK)'
        end as b_referansePVK,
        id as b_id,
        concat('https://behandlingskatalog.ansatt.nav.no/process/purpose/', purpose_code, '/', id) as b_url_behkat,
        concat('https://etterlevelse.ansatt.nav.no/dokumentasjoner?tab=behandlingsok&behandlingId=', id) as b_url_etterlevelse,
    from `teamdatajegerne-prod-c8b1.Behandlingskatalog_Publisering.Behandling`
    left join unnest(affiliation_productTeams) as team_id
    left join teams_i_omrader on team_id = teams_i_omrader.team_id
    cross join unnest(json_extract_array(affiliation_products)) as products
    where active = true
),

kobling_mellom_E_og_B as (
    select
        etterlevelseDokumentasjonsId as e_id,
        behandlingId
    from `teamdatajegerne-prod-c8b1.Etterlevelse_Publisering.BehandlingStatistikk`
    group by etterlevelseDokumentasjonsId, behandlingId
),

edok as (
    select
        concat('E', etterlevelseNummer) as e_nr,
        coalesce(teams_i_omrader.team_navn, 'E-nr uten team') as e_team_navn,
        coalesce(teams_i_omrader.team_omrade_navn, 'E-nr uten team-område') as e_team_omrade_navn,
        etterlevelsedokumentasjonid as e_id,
        kobling_mellom_E_og_B.behandlingId as b_id,
    from `teamdatajegerne-prod-c8b1.etterlevelse.ds_dokument`
    left join unnest(teams) as team_id
    left join teams_i_omrader on team_id = teams_i_omrader.team_id
    left join kobling_mellom_E_og_B on kobling_mellom_E_og_B.e_id = etterlevelsedokumentasjonid
    where aktivRad = true
),

bdok_med_e_nr as (
    select
        bdok.*,
        e_nr,
        e_team_navn,
        e_team_omrade_navn
    from bdok
    left join edok on edok.b_id = bdok.b_id
    order by e_nr desc
),

b_nr_aggregert as (
    select
        b_nr,
        b_avdeling,
        b_omrade,
        string_agg(distinct b_team_navn, ', ' order by b_team_navn) as b_teams,
        string_agg(distinct b_team_omrade_navn, ', ' order by b_team_omrade_navn) as b_team_omrader,
        string_agg(distinct e_nr, ', ' order by e_nr asc) as koblede_e_nr,
        string_agg(distinct e_team_navn, ', ' order by e_team_navn) as koblede_e_nr_teams,
        string_agg(distinct e_team_omrade_navn, ', ' order by e_team_omrade_navn) as koblede_e_nr_omrader,
        b_navn,
        b_tema,
        b_status,
        relaterte_systemer,
        b_behovForPVK,
        b_referansePVK,
        nr,
        b_url_behkat,
        b_url_etterlevelse,
    from bdok_med_e_nr
    group by
        b_nr,
        b_avdeling,
        b_omrade,
        b_navn,
        b_tema,
        b_status,
        relaterte_systemer,
        b_behovForPVK,
        b_referansePVK,
        nr,
        b_url_behkat,
        b_url_etterlevelse
)

select * from b_nr_aggregert
