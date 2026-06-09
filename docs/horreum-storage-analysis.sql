-- Horreum Storage Analysis for a specific test
-- Usage: psql -h <host> -p <port> -U <user> -d horreum -v testid=339 -v run_limit=10 -f horreum-storage-analysis.sql

-- Parameters (change these or pass via -v):
-- :testid    - the Horreum test ID
-- :run_limit - how many recent runs to analyze (use 0 for all)

\echo '============================================='
\echo 'Horreum Storage Analysis'
\echo '============================================='
\echo ''

-- Select the runs to analyze
CREATE TEMP TABLE analyzed_runs AS
SELECT id FROM run
WHERE testid = :testid AND trashed = false
ORDER BY id DESC
LIMIT CASE WHEN :run_limit > 0 THEN :run_limit ELSE 2147483647 END;

CREATE TEMP TABLE analyzed_datasets AS
SELECT d.id FROM dataset d
WHERE d.runid IN (SELECT id FROM analyzed_runs);

\echo '--- Scope ---'
SELECT :testid as test_id, count(*) as runs_analyzed FROM analyzed_runs;

\echo ''
\echo '--- Per-table storage ---'

SELECT 'run (raw upload data)' as table_name,
  count(*) as rows,
  pg_size_pretty(sum(pg_column_size(r.*))::bigint) as total_size,
  pg_size_pretty(sum(pg_column_size(r.data))::bigint) as data_column,
  sum(pg_column_size(r.*)) as total_bytes
FROM run r WHERE r.id IN (SELECT id FROM analyzed_runs)

UNION ALL
SELECT 'dataset (extracted data)',
  count(*),
  pg_size_pretty(sum(pg_column_size(d.*))::bigint),
  pg_size_pretty(sum(pg_column_size(d.data))::bigint),
  sum(pg_column_size(d.*))
FROM dataset d WHERE d.runid IN (SELECT id FROM analyzed_runs)

UNION ALL
SELECT 'label_values (computed)',
  count(*),
  pg_size_pretty(sum(pg_column_size(lv.*))::bigint),
  pg_size_pretty(coalesce(sum(pg_column_size(lv.value))::bigint, 0)),
  sum(pg_column_size(lv.*))
FROM label_values lv WHERE lv.dataset_id IN (SELECT id FROM analyzed_datasets)

UNION ALL
SELECT 'fingerprint',
  count(*),
  pg_size_pretty(sum(pg_column_size(fp.*))::bigint),
  pg_size_pretty(coalesce(sum(pg_column_size(fp.fingerprint))::bigint, 0)),
  sum(pg_column_size(fp.*))
FROM fingerprint fp WHERE fp.dataset_id IN (SELECT id FROM analyzed_datasets)

UNION ALL
SELECT 'datapoint',
  count(*),
  pg_size_pretty(coalesce(sum(pg_column_size(dp.*))::bigint, 0)),
  '-',
  coalesce(sum(pg_column_size(dp.*)), 0)
FROM datapoint dp WHERE dp.dataset_id IN (SELECT id FROM analyzed_datasets)

UNION ALL
SELECT 'change (detected changes)',
  count(*),
  pg_size_pretty(coalesce(sum(pg_column_size(c.*))::bigint, 0)),
  '-',
  coalesce(sum(pg_column_size(c.*)), 0)
FROM change c WHERE c.dataset_id IN (SELECT id FROM analyzed_datasets)

UNION ALL
SELECT 'changedetection (config)',
  count(*),
  pg_size_pretty(coalesce(sum(pg_column_size(cd.*))::bigint, 0)),
  '-',
  coalesce(sum(pg_column_size(cd.*)), 0)
FROM changedetection cd
WHERE cd.variable_id IN (SELECT v.id FROM variable v WHERE v.testid = :testid)

ORDER BY total_bytes DESC;

\echo ''
\echo '--- Total ---'
SELECT pg_size_pretty(total::bigint) as total_storage, total as total_bytes
FROM (
  SELECT
    coalesce((SELECT sum(pg_column_size(r.*)) FROM run r WHERE r.id IN (SELECT id FROM analyzed_runs)), 0) +
    coalesce((SELECT sum(pg_column_size(d.*)) FROM dataset d WHERE d.runid IN (SELECT id FROM analyzed_runs)), 0) +
    coalesce((SELECT sum(pg_column_size(lv.*)) FROM label_values lv WHERE lv.dataset_id IN (SELECT id FROM analyzed_datasets)), 0) +
    coalesce((SELECT sum(pg_column_size(fp.*)) FROM fingerprint fp WHERE fp.dataset_id IN (SELECT id FROM analyzed_datasets)), 0) +
    coalesce((SELECT sum(pg_column_size(dp.*)) FROM datapoint dp WHERE dp.dataset_id IN (SELECT id FROM analyzed_datasets)), 0) +
    coalesce((SELECT sum(pg_column_size(c.*)) FROM change c WHERE c.dataset_id IN (SELECT id FROM analyzed_datasets)), 0) +
    coalesce((SELECT sum(pg_column_size(cd.*)) FROM changedetection cd WHERE cd.variable_id IN (SELECT v.id FROM variable v WHERE v.testid = :testid)), 0)
    as total
) t;

\echo ''
\echo '--- Raw data vs computed ---'
SELECT
  pg_size_pretty(raw_bytes::bigint) as raw_upload_data,
  pg_size_pretty((total_bytes - raw_bytes)::bigint) as computed_data,
  round(total_bytes::numeric / raw_bytes, 1) as overhead_ratio
FROM (
  SELECT
    coalesce((SELECT sum(pg_column_size(r.data)) FROM run r WHERE r.id IN (SELECT id FROM analyzed_runs)), 0) as raw_bytes,
    coalesce((SELECT sum(pg_column_size(r.*)) FROM run r WHERE r.id IN (SELECT id FROM analyzed_runs)), 0) +
    coalesce((SELECT sum(pg_column_size(d.*)) FROM dataset d WHERE d.runid IN (SELECT id FROM analyzed_runs)), 0) +
    coalesce((SELECT sum(pg_column_size(lv.*)) FROM label_values lv WHERE lv.dataset_id IN (SELECT id FROM analyzed_datasets)), 0) +
    coalesce((SELECT sum(pg_column_size(fp.*)) FROM fingerprint fp WHERE fp.dataset_id IN (SELECT id FROM analyzed_datasets)), 0) +
    coalesce((SELECT sum(pg_column_size(dp.*)) FROM datapoint dp WHERE dp.dataset_id IN (SELECT id FROM analyzed_datasets)), 0) +
    coalesce((SELECT sum(pg_column_size(c.*)) FROM change c WHERE c.dataset_id IN (SELECT id FROM analyzed_datasets)), 0) +
    coalesce((SELECT sum(pg_column_size(cd.*)) FROM changedetection cd WHERE cd.variable_id IN (SELECT v.id FROM variable v WHERE v.testid = :testid)), 0)
    as total_bytes
) t;

DROP TABLE analyzed_runs;
DROP TABLE analyzed_datasets;
