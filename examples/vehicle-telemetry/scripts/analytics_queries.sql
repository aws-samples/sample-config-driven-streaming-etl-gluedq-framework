-- ============================================================================
-- Vehicle Telemetry - Sample Athena Analytics Queries
-- ============================================================================
-- Run these in the Athena console after deploying the vehicle-telemetry use case.
-- Database: <stack_name with hyphens replaced by underscores>_db
--   e.g. stack "fleet-demo" -> database "fleet_demo_db"
--   (Athena identifiers cannot contain hyphens, so deploy.sh sanitizes the name.)
-- Set the Athena "Database" dropdown to this database, or fully-qualify tables.
-- ============================================================================

-- 1. Current vehicle fleet status (SCD2 current records only)
SELECT id, vin, make, model, year, fuel_type, status, _effective_from
FROM vehicles
WHERE _is_current = true
ORDER BY id;

-- 2. Latest telemetry per vehicle
SELECT v.vin, v.make, v.model,
       t.latitude, t.longitude, t.speed_kmh, t.fuel_level_pct,
       t.engine_temp_c, t.engine_status, t._ingested_at
FROM vehicle_telemetry t
JOIN vehicles v ON t.vehicle_id = v.id AND v._is_current = true
WHERE t._is_current = true
ORDER BY t._ingested_at DESC;

-- 3. DQ quarantine analysis — what's getting quarantined and why
-- (Query the quarantine S3 bucket via Athena if external table is set up)

-- 4. Glue Data Quality results summary — DQDL rule outcomes over time
SELECT table_name, rule,
       COUNT(*) as evaluations,
       SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed_batches,
       SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) as failed_batches,
       MAX(timestamp) as last_evaluated
FROM dq_metrics
GROUP BY table_name, rule
ORDER BY table_name, rule;

-- 5. Vehicle status change history (SCD2 timeline)
SELECT id, vin, status, _effective_from, _effective_to, _is_current, _operation
FROM vehicles
WHERE vin = '1HGBH41JXMN109186'
ORDER BY _effective_from;

-- 6. Delivery pipeline status
SELECT status, COUNT(*) as count
FROM deliveries
WHERE _is_current = true
GROUP BY status
ORDER BY count DESC;

-- 7. Alert frequency by type
SELECT alert_type, severity, COUNT(*) as count
FROM alerts
WHERE _is_current = true
GROUP BY alert_type, severity
ORDER BY count DESC;

-- 8. Speed violations (records that passed DQ but are near limits)
SELECT vehicle_id, speed_kmh, latitude, longitude, _ingested_at
FROM vehicle_telemetry
WHERE _is_current = true AND speed_kmh > 120
ORDER BY speed_kmh DESC
LIMIT 20;
