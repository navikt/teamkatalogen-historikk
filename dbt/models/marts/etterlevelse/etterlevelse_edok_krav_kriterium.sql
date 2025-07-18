-- etterlevelse_edok_krav_kriterium


with

teams_i_omrader as (
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
        avdeling as avdeling,
        etterlevelsedokumentasjonid as e_id,
        concat('E', etterlevelseNummer) as e_nr,
        array_to_string(teams, ', ') as e_team_id_string,
        concat('https://etterlevelse.ansatt.nav.no/dokumentasjon/', etterlevelsedokumentasjonid) as e_url,
    -- from {{ source('datajegerne', 'ds_dokument') }}
    from `teamdatajegerne-prod-c8b1.etterlevelse.ds_dokument`
    where aktivRad = true
),

krav_kriterier as (
    select
        tema as krav_tema,
        kravNummer as krav,
        kravoppfylt as krav_oppfylt,
        kravFerdigUtfylt as krav_ferdig_utfylt,
        case
            when kravoppfylt = true and kravFerdigUtfylt = true then 'Oppfylt og dokumentert'
            when kravoppfylt = false and kravFerdigUtfylt = true then 'Ikke oppfylt, men ferdig dokumentert'
            when kravoppfylt = false and kravFerdigUtfylt = false then 'Ikke oppfylt, ikke dokumentert ferdig'
        end as krav_status,
        begrunnelse as kriterie_begrunnelse,
        etterlevelseDokumentasjonId as e_id,
        suksesskriterieStatus as kriterie_status,
        concat('K', kravNummer, ' - ', suksesskriterieId) as kriterie
        -- from {{ source('datajegerne', 'ds_besvarelser_tema')}}
        from `teamdatajegerne-prod-c8b1.etterlevelse.ds_besvarelser_tema`
        where
            aktivRad = true
            and tema is not null -- fjerner gamle temaer som ikke er i bruk
),

sammensmeltet as (
    select
        edok.e_navn,
        edok.avdeling,
        edok.e_id,
        edok.e_nr,
        edok.e_team_id_string,
        edok.e_url,
        krav_kriterier.krav_tema,
        krav_kriterier.krav,
        krav_kriterier.krav_oppfylt,
        krav_kriterier.krav_ferdig_utfylt,
        krav_kriterier.krav_status,
        krav_kriterier.kriterie_begrunnelse,
        krav_kriterier.kriterie_status,
        krav_kriterier.kriterie
    from edok
    inner join krav_kriterier using (e_id)
),


per_team as (
    select
        sammensmeltet.e_navn,
        sammensmeltet.avdeling,
        sammensmeltet.e_id,
        sammensmeltet.e_nr,
        sammensmeltet.e_team_id_string,
        sammensmeltet.e_url,
        sammensmeltet.krav_tema,
        sammensmeltet.krav,
        sammensmeltet.krav_oppfylt,
        sammensmeltet.krav_ferdig_utfylt,
        sammensmeltet.krav_status,
        sammensmeltet.kriterie_begrunnelse,
        sammensmeltet.kriterie_status,
        sammensmeltet.kriterie,
        coalesce(teams_i_omrader.team_navn, 'E-nr uten team') as team_navn,
        teams_i_omrader.omrade_navn,
        teams_i_omrader.team_id,
        teams_i_omrader.omrade_id
    from sammensmeltet
    left join teams_i_omrader on sammensmeltet.e_team_id_string like '%' || teams_i_omrader.team_id || '%'
),

final as (
    select
        e_nr,
        kriterie,
        kriterie_status,
        krav,
        krav_oppfylt,
        krav_ferdig_utfylt,
        krav_status,
        krav_tema,
        e_navn,
        team_navn,
        omrade_navn,
        avdeling,
        kriterie_begrunnelse,
        e_url,
        e_id
        -- team_id,
        -- omrade_id
        -- e_team_id_string,
    from per_team
)

select * from final
