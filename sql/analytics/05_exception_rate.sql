-- KPI-B4: exception rate per UTC day, plus the whole range (activity_date IS NULL):
-- package-days with an exception / active package-days.
SELECT activity_date,
       sum(active_packages)          AS active_packages,
       sum(packages_with_exception)  AS packages_with_exception,
       round(100.0 * sum(packages_with_exception) / sum(active_packages), 2) AS exception_rate_pct
FROM analytics.daily_network_activity
WHERE activity_date BETWEEN :'from_date'::date AND :'to_date'::date
GROUP BY GROUPING SETS ((activity_date), ())
ORDER BY activity_date NULLS LAST;
