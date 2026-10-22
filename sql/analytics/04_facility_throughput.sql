-- Facility throughput over [from_date, to_date] (UTC activity dates). avg_arrivals_per_day divides by the
-- number of days in the range, so days without activity count as zero.
SELECT facility_id,
       sum(arrivals)         AS arrivals,
       sum(departures)       AS departures,
       sum(exception_count)  AS exceptions,
       round(sum(arrivals)::numeric / (:'to_date'::date - :'from_date'::date + 1), 2) AS avg_arrivals_per_day
FROM analytics.daily_facility_performance
WHERE activity_date BETWEEN :'from_date'::date AND :'to_date'::date
GROUP BY facility_id
ORDER BY facility_id;
