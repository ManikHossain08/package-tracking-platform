-- KPI-B1: on-time delivery rate by service level over [from_date, to_date] (delivery_date, destination-local).
-- Total row: service_level IS NULL. A window with no deliveries returns only the total row, with on_time_pct NULL.
SELECT service_level,
       count(*)                                                       AS delivered,
       count(*) FILTER (WHERE on_time)                                AS on_time,
       round(100.0 * count(*) FILTER (WHERE on_time) / NULLIF(count(*), 0), 2) AS on_time_pct
FROM analytics.delivery_facts
WHERE delivery_date BETWEEN :'from_date'::date AND :'to_date'::date
GROUP BY ROLLUP (service_level)
ORDER BY service_level NULLS LAST;
