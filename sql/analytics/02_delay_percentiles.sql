-- KPI-B2: signed delivery delay in minutes (negative = early) by service level; exact percentiles.
SELECT service_level,
       count(*)                                                                         AS delivered,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY delay_minutes)::numeric, 1)   AS delay_p50_min,
       round(percentile_cont(0.9)  WITHIN GROUP (ORDER BY delay_minutes)::numeric, 1)   AS delay_p90_min,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY delay_minutes)::numeric, 1)   AS delay_p95_min,
       round(max(delay_minutes)::numeric, 1)                                            AS delay_max_min
FROM analytics.delivery_facts
WHERE delivery_date BETWEEN :'from_date'::date AND :'to_date'::date
GROUP BY service_level
ORDER BY service_level;
