-- Fixed fixture for the SQL query pack (TS-D-05). The expected results are computed by hand in AnalyticsQueryPackIT.
-- Range under test: 2026-10-20 .. 2026-10-21. Rows dated 2026-10-22/23 must be filtered out.
INSERT INTO analytics.delivery_facts VALUES
  ('PKG-FIXTURE00001', '2026-10-20', '2026-10-21T02:00:00Z', '2026-10-20', 'STANDARD', 'H2X', 'America/Toronto',  -60, true,  now(), 'fixture'),
  ('PKG-FIXTURE00002', '2026-10-20', '2026-10-21T04:30:00Z', '2026-10-19', 'STANDARD', 'H2X', 'America/Toronto',   30, false, now(), 'fixture'),
  ('PKG-FIXTURE00003', '2026-10-20', '2026-10-21T03:50:00Z', '2026-10-20', 'EXPRESS',  'M5V', 'America/Toronto',  -10, true,  now(), 'fixture'),
  ('PKG-FIXTURE00004', '2026-10-21', '2026-10-22T02:00:00Z', '2026-10-21', 'STANDARD', 'V6B', 'America/Vancouver',-120, true,  now(), 'fixture'),
  ('PKG-FIXTURE00005', '2026-10-22', '2026-10-23T05:30:00Z', '2026-10-21', 'EXPRESS',  'M5V', 'America/Toronto',   90, false, now(), 'fixture');

INSERT INTO analytics.facility_visits VALUES
  ('visit-1', 'PKG-FIXTURE00001', 'DC-YUL01', '2026-10-20T08:00:00Z', '2026-10-20', '2026-10-20T09:00:00Z', '2026-10-20',  60, now(), 'fixture'),
  ('visit-2', 'PKG-FIXTURE00002', 'DC-YUL01', '2026-10-20T08:00:00Z', '2026-10-20', '2026-10-20T11:00:00Z', '2026-10-20', 180, now(), 'fixture'),
  ('visit-3', 'PKG-FIXTURE00003', 'DC-YUL01', '2026-10-21T08:00:00Z', '2026-10-21', '2026-10-21T10:00:00Z', '2026-10-21', 120, now(), 'fixture'),
  ('visit-4', 'PKG-FIXTURE00004', 'DC-YYZ01', '2026-10-21T08:00:00Z', '2026-10-21', '2026-10-21T08:30:00Z', '2026-10-21',  30, now(), 'fixture'),
  ('visit-5', 'PKG-FIXTURE00005', 'DC-YYZ01', '2026-10-21T09:00:00Z', '2026-10-21', NULL, NULL, NULL, now(), 'fixture'),
  ('visit-6', 'PKG-FIXTURE00006', 'DC-YYZ01', '2026-10-22T09:00:00Z', '2026-10-22', '2026-10-23T01:39:00Z', '2026-10-23', 999, now(), 'fixture');

INSERT INTO analytics.daily_facility_performance VALUES
  ('2026-10-20', 'DC-YUL01', 4, 2, 120, 177, 1, now(), 'fixture'),
  ('2026-10-21', 'DC-YUL01', 2, 1, 120, 120, 0, now(), 'fixture'),
  ('2026-10-21', 'DC-YYZ01', 3, 1,  30,  30, 2, now(), 'fixture'),
  ('2026-10-22', 'DC-YUL01', 9, 9, NULL, NULL, 9, now(), 'fixture');

INSERT INTO analytics.daily_network_activity VALUES
  ('2026-10-20', 10, 1, 0.1,  now(), 'fixture'),
  ('2026-10-21', 20, 3, 0.15, now(), 'fixture'),
  ('2026-10-22',  5, 5, 1.0,  now(), 'fixture');
