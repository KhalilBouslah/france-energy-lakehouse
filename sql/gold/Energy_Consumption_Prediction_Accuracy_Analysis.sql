
SELECT to_date(from_utc_timestamp(date_heure, 'Europe/Paris')) AS day,
       round(100 * avg(abs(consommation - prevision_j1) / consommation), 2) AS error_day_ahead_pct,
       round(100 * avg(abs(consommation - prevision_j) / consommation), 2) AS error_intraday_pct
FROM energy.energy.fr_enrgy
WHERE consommation > 0 AND prevision_j IS NOT NULL AND prevision_j1 IS NOT NULL
GROUP BY 1

