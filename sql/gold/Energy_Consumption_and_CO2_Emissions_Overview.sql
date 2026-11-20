
SELECT to_date(from_utc_timestamp(date_heure, 'Europe/Paris')) AS day,
       round(avg(consommation) * 24 / 1000, 1) AS consumption_gwh,
       max(consommation) AS peak_mw,
       round(avg(taux_co2), 1) AS avg_co2_g_kwh,
       round(100 * sum(coalesce(eolien,0)+coalesce(solaire,0)+coalesce(hydraulique,0)+coalesce(bioenergies,0))
         / nullif(sum(coalesce(nucleaire,0)+coalesce(eolien,0)+coalesce(solaire,0)+coalesce(hydraulique,0)
                    + coalesce(bioenergies,0)+coalesce(gaz,0)+coalesce(fioul,0)+coalesce(charbon,0)), 0), 1) AS renewable_share_pct,
       round(avg(nucleaire)*24/1000,1) AS nuclear_gwh,
       round(avg(eolien)*24/1000,1) AS wind_gwh,
       round(avg(solaire)*24/1000,1) AS solar_gwh,
       round(avg(hydraulique)*24/1000,1) AS hydro_gwh,
       round(avg(bioenergies)*24/1000,1) AS bio_gwh,
       round(avg(gaz)*24/1000,1) AS gas_gwh,
       round(avg(fioul)*24/1000,1) AS oil_gwh,
       round(avg(charbon)*24/1000,1) AS coal_gwh
FROM energy.energy.fr_enrgy
WHERE consommation IS NOT NULL
GROUP BY 1


