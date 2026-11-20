Build an AI/BI dashboard titled "France Energy Lakehouse: live electricity mix".
Use ONLY these existing datasets: ds_live, ds_latest, ds_daily, ds_forecast_error, ds_exchanges, ds_quality.
Do not create, modify or query any other table.

Context:
- Data: RTE eCO2mix (France national electricity), updated hourly.
- Power columns are in MW (15-minute averages). CO2 is in gCO2/kWh. Energy columns ending in _gwh are in GWh.
- All time columns are already in Paris time (ts_paris, day, last_update_paris).
- In ds_live, rows where consommation is null are future forecast slots: show them only in the forecast chart.
- In ds_exchanges, negative values mean France exports.

Page 1 "Live":
1. Four counters from ds_latest: consumption_mw ("Consumption now, MW"), co2_g_kwh ("CO2 intensity, g/kWh"),
   renewable_share_pct ("Renewable share, %"), last_update_paris ("Last update, Paris time").
2. Donut from ds_latest: current production mix (nucleaire, eolien, solaire, hydraulique, bioenergies, gaz, fioul, charbon).
3. Stacked area from ds_live: production by source over the last 7 days, x = ts_paris, y = MW.
4. Line chart from ds_live, last 48 hours: consommation (actual) vs prevision_j1 (day-ahead forecast) vs prevision_j (intraday forecast).
5. Line chart from ds_live: taux_co2 over the last 7 days.

Page 2 "Trends":
1. Line from ds_daily: consumption_gwh per day.
2. Line from ds_daily: renewable_share_pct per day, with a 30-day moving average if possible.
3. Bar from ds_daily: average avg_co2_g_kwh per month.
4. Stacked bar from ds_daily: monthly energy by source (nuclear_gwh, wind_gwh, solar_gwh, hydro_gwh, bio_gwh, gas_gwh, oil_gwh, coal_gwh).
5. Line from ds_daily: peak_mw per day.

Page 3 "Forecasts & exchanges":
1. Line from ds_forecast_error: error_day_ahead_pct and error_intraday_pct per day, plus a counter with the average over the selected period.
2. Stacked bar from ds_exchanges: daily exchanges by country (uk, spain, italy, switzerland, germany_belgium), negative = exports.

Page 4 "Data quality":
1. Counter from ds_latest: minutes_since_update ("Minutes since last real value"), red if above 120.
2. Bar from ds_quality: rows_with_values per day, with a reference line at 96 (expected 15-min slots per day).
3. Stacked bar from ds_quality: definitive_rows, consolidated_rows, realtime_rows per month.

Global filter: a date range on "day" (default: last 90 days) applied to ds_daily, ds_forecast_error, ds_exchanges and ds_quality.

Style:
- Same color per source on every chart: nuclear #F2C14E, wind #4CB5AE, solar #F79D65, hydro #3E7CB1,
  bioenergy #6A994E, gas #9D8DF1, oil #8C6E63, coal #555555.
- Short titles, units in axis titles, no decimals on MW values.
- Add a text box at the bottom of each page: "Data: RTE eCO2mix via ODRÉ · Times in Europe/Paris · Updated hourly".

If you can't build everything at once, build Page 1 first and tell me what's left.
