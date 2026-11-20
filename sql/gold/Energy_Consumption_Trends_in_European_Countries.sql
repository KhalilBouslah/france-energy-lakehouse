

SELECT to_date(from_utc_timestamp(date_heure, 'Europe/Paris')) AS day,
       round(avg(ech_comm_angleterre)*24/1000,1) AS uk_gwh,
       round(avg(ech_comm_espagne)*24/1000,1) AS spain_gwh,
       round(avg(ech_comm_italie)*24/1000,1) AS italy_gwh,
       round(avg(ech_comm_suisse)*24/1000,1) AS switzerland_gwh,
       round(avg(ech_comm_allemagne_belgique)*24/1000,1) AS germany_belgium_gwh
FROM energy.energy.fr_enrgy
WHERE consommation IS NOT NULL
GROUP BY 1

