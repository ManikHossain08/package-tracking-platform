-- KPI-B3: dwell time per facility for visits that departed in [from_date, to_date] (UTC departure date).
SELECT facility_id,
       count(*)                                                                         AS completed_visits,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY dwell_minutes)::numeric, 1)   AS dwell_p50_min,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY dwell_minutes)::numeric, 1)   AS dwell_p95_min
FROM analytics.facility_visits
WHERE departure_date BETWEEN :'from_date'::date AND :'to_date'::date
  AND dwell_minutes IS NOT NULL
GROUP BY facility_id
ORDER BY facility_id;
