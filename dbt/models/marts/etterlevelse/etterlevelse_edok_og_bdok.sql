-- etterlevelsesdokumenter


with

ref_teams_i_omrader as (
    select
        team_navn,
        omrade_navn,
        team_id,
        omrade_id
    -- from {{ ref('teams_i_omrader') }}
    from `pensjon-saksbehandli-prod-1f83.teamkatalogen_historikk.teams_i_omrader`
),

edok as (
    select
        title as e_navn,
        avdeling as e_avdeling,
        etterlevelsedokumentasjonid as e_id,
        concat('E', etterlevelseNummer) as e_nr,
        coalesce(array_to_string(teams, ', '), 'E-nr uten team') as e_team_id_string,
        concat('https://etterlevelse.ansatt.nav.no/dokumentasjon/', etterlevelsedokumentasjonid) as e_url,
    from `teamdatajegerne-prod-c8b1.etterlevelse.ds_dokument`
    where aktivRad = true -- and array_length(teams) > 0
),

-- kan evt brukes senere for mer info om E-nr per B-nr
-- krav_kriterier as (
--     select
--         tema as krav_tema,
--         kravNummer as krav,
--         kravoppfylt as krav_oppfylt,
--         kravFerdigUtfylt as krav_ferdig_utfylt,
--         begrunnelse as kriterie_begrunnelse,
--         etterlevelseDokumentasjonId as e_id,
--         suksesskriterieStatus as kriterie_status,
--         concat('K', kravNummer, ' - ', suksesskriterieId) as kriterie
--         from `teamdatajegerne-prod-c8b1.etterlevelse.ds_besvarelser_tema`
--         where aktivRad = true
-- ),

bdok as (
    select
        id as b_id,
        coalesce(array_to_string(affiliation_productTeams, ', '), 'B-nr uten team') as b_team_id_string,
        concat('B', number) as b_nr,
        name as b_navn,
        status as b_status,
        active as b_aktiv,
        purpose_shortName as b_tema,
        dpia_needForDpia as b_behovForPVK,
        dpia_refToDpia as b_referansePVK,
        concat('https://behandlingskatalog.ansatt.nav.no/process/purpose/', purpose_code, '/', id) as b_url_behkat,
        concat('https://etterlevelse.ansatt.nav.no/dokumentasjoner/behandlingsok?behandlingId=', id) as b_url_etterlevelse,
    from `teamdatajegerne-prod-c8b1.Behandlingskatalog_Publisering.Behandling`
    where active = true
),

kobling_mellom_E_og_B as (
    select
        etterlevelseDokumentasjonsId as e_id,
        behandlingId as b_id,
        antallIkkeFiltrertKrav as b_krav_relevant,
        antallFerdigDokumentert as b_krav_dokumentert,
        antallUnderArbeid as b_krav_dok_under_arbeid,
        antallIkkePaabegynt as b_krav_dok_ikke_paabegynt
    from `teamdatajegerne-prod-c8b1.Etterlevelse_Publisering.BehandlingStatistikk`
),

b_per_team as (
    select
        bdok.b_id,
        bdok.b_navn,
        bdok.b_nr,
        bdok.b_status,
        bdok.b_aktiv,
        bdok.b_tema,
        bdok.b_behovForPVK,
        bdok.b_referansePVK,
        bdok.b_url_behkat,
        bdok.b_url_etterlevelse,
        ref_teams_i_omrader.team_navn as b_team_navn,
        ref_teams_i_omrader.omrade_navn as b_omrade_navn,
        ref_teams_i_omrader.team_id as b_team_id
    from bdok
    left join ref_teams_i_omrader on bdok.b_team_id_string like '%' || ref_teams_i_omrader.team_id || '%'
),

-- kan evt brukes senere for mer info om E-nr per B-nr
-- e_info_sammensmeltet as (
--     select
--         -- edok.e_navn,
--         edok.e_avdeling,
--         edok.e_id,
--         edok.e_nr,
--         -- edok.e_team_id_string,
--         -- edok.e_url,
--         krav_kriterier.krav_tema,
--         krav_kriterier.krav,
--         krav_kriterier.krav_oppfylt,
--         krav_kriterier.krav_ferdig_utfylt,
--         krav_kriterier.kriterie_begrunnelse,
--         krav_kriterier.kriterie_status,
--         krav_kriterier.kriterie
--     from edok
--     inner join krav_kriterier using (e_id)
-- ),

b_koblet_til_e as (
    select
        b_per_team.b_id,
        b_per_team.b_navn,
        b_per_team.b_nr,
        b_per_team.b_status,
        b_per_team.b_aktiv,
        b_per_team.b_tema,
        b_per_team.b_behovForPVK,
        b_per_team.b_referansePVK,
        b_per_team.b_url_behkat,
        b_per_team.b_url_etterlevelse,
        b_per_team.b_team_navn,
        b_per_team.b_omrade_navn,
        b_per_team.b_team_id,
        kobling_mellom_E_og_B.e_id,
        kobling_mellom_E_og_B.b_krav_relevant,
        kobling_mellom_E_og_B.b_krav_dokumentert,
        kobling_mellom_E_og_B.b_krav_dok_under_arbeid,
        kobling_mellom_E_og_B.b_krav_dok_ikke_paabegynt,
        edok.e_nr,
        edok.e_navn,
        edok.e_avdeling
    from b_per_team
    left join kobling_mellom_E_og_B using (b_id)
    left join edok using (e_id)
),

final as (
    select
        b_nr,
        e_nr,
        b_team_navn,
        b_omrade_navn,
        e_avdeling,
        b_navn,
        b_status,
        b_aktiv,
        b_tema,
        b_behovForPVK,
        b_referansePVK,
        b_krav_relevant,
        b_krav_dokumentert,
        b_krav_dok_under_arbeid,
        b_krav_dok_ikke_paabegynt,
        e_navn,
        b_team_id,
        b_url_behkat,
        b_url_etterlevelse,
        b_id,
        e_id
    from b_koblet_til_e
)

select * from final
