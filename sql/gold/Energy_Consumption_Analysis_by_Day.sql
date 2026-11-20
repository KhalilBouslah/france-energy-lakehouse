

SELECT to_date(from_utc_timestamp(date_heure, 'Europe/Paris')) AS day,
       count(*) AS rows_total,
       count(consommation) AS rows_with_values,
       count(*) - count(consommation) AS forecast_rows,
       count_if(lower(nature) LIKE '%finitive%') AS definitive_rows,
       count_if(lower(nature) LIKE '%consolid%') AS consolidated_rows,
       count_if(lower(nature) LIKE '%temps r%') AS realtime_rows
FROM energy.energy.fr_enrgy
GROUP BY 1
