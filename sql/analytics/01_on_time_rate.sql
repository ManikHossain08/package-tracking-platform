-- KPI-B1: on-time delivery rate by service level over [from_date, to_date] (delivery_date, destination-local).
-- Total row: service_level IS NULL.
SELECT service_level,
       count(*)                                                       AS delivered,
       count(*) FILTER (WHERE on_time)                                AS on_time,
       round(100.0 * count(*) FILTER (WHERE on_time) / count(*), 2)   AS on_time_pct
FROM analytics.delivery_facts
WHERE delivery_date BETWEEN :'from_date'::date AND :'to_date'::date
GROUP BY ROLLUP (service_level)
ORDER BY service_level NULLS LAST;
