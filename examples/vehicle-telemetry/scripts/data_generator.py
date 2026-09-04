"""
Vehicle Telemetry Data Generator Lambda.

COMPLIANCE NOTE: This generator produces synthetic personal data (customer names
and phone numbers) purely to demonstrate the pipeline. Customer contact fields
are masked before storage by the `mask_pii` transforms on the `deliveries` table
in config/tables.yaml. If you adapt this for real personal data, you are
responsible for your own GDPR/privacy safeguards.


Generates diverse data that exercises the full DQ/SCD2/quality-metrics pipeline:
- ~5-10% of records trigger DQ quarantine
- ~20-30% of operations are updates/deletes (not just inserts)
- ~10% of batches trigger Glue DQ threshold warnings

Usage:
  {"action": "seed", "vehicles": 15, "drivers": 10}
  {"action": "generate", "duration_seconds": 300, "interval_seconds": 2}
  {"action": "burst", "records": 200}
"""
from __future__ import annotations

import json
import math
import os
# OS-entropy-backed RNG (CWE-338 / Bandit B311 compliant). Test-data generation
# has no cryptographic requirement, but SystemRandom keeps security scans clean
# with the identical API (choice/randint/random/sample/uniform).
from random import SystemRandom

rng = SystemRandom()
import secrets
import string
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import psycopg2
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_NAME = "etldb"
DEFAULT_DB_USER = "postgres"

DQ_VIOLATION_RATE = 0.08       # ~8% of records get a DQ violation
SCD2_UPDATE_RATE = 0.25        # ~25% of operations are updates/deletes
DQ_TRIGGER_RATE = 0.10      # ~10% of batches trigger DQ warnings

VALID_DELIVERY_STATUSES = ["assigned", "picked_up", "in_transit", "delivered", "failed"]
VALID_ALERT_TYPES = ["speeding", "harsh_braking", "low_fuel", "engine_overheat", "geofence_violation"]

# NYC area zones for realistic routes
ZONES: Dict[str, Dict[str, Tuple[float, float]]] = {
    "manhattan": {"lat": (40.70, 40.82), "lon": (-74.02, -73.93)},
    "brooklyn": {"lat": (40.57, 40.70), "lon": (-74.04, -73.85)},
    "queens": {"lat": (40.68, 40.78), "lon": (-73.96, -73.70)},
}

MAKES_MODELS = [
    ("Ford", "Transit"), ("Ford", "E-Transit"), ("Toyota", "Hiace"),
    ("Mercedes", "Sprinter"), ("Chevrolet", "Express"), ("RAM", "ProMaster"),
]
FUEL_TYPES = ["diesel", "diesel", "electric", "hybrid", "gasoline"]
FIRST_NAMES = ["John", "Maria", "James", "Sarah", "Michael", "Emily", "Robert", "Jennifer",
               "David", "Lisa", "William", "Jessica", "Daniel", "Amanda", "Thomas", "Ashley"]
LAST_NAMES = ["Smith", "Garcia", "Wilson", "Johnson", "Brown", "Davis", "Miller", "Taylor",
              "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Lee"]
STREETS = ["Main St", "Oak Ave", "Park Blvd", "Broadway", "5th Ave", "Madison Ave",
           "Lexington Ave", "Canal St", "Houston St", "Fulton St"]

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_cached_credentials: Optional[Dict[str, str]] = None
VEHICLE_STATES: Dict[int, Dict[str, Any]] = {}
ACTIVE_DELIVERIES: Dict[int, Dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db_credentials() -> Dict[str, str]:
    """Fetch database credentials from Secrets Manager (required)."""
    global _cached_credentials
    if _cached_credentials is not None:
        return _cached_credentials

    secret_arn = os.environ.get("RDS_SECRET_ARN")
    rds_host = os.environ.get("RDS_HOST", os.environ.get("DB_HOST", DEFAULT_DB_HOST))
    if not secret_arn:
        raise RuntimeError(
            "RDS_SECRET_ARN environment variable is required. "
            "Ensure the Lambda is deployed with the correct environment configuration."
        )

    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)
    secret_data = json.loads(response.get("SecretString", "{}"))
    _cached_credentials = {
        "host": secret_data.get("host", rds_host),
        "dbname": secret_data.get("dbname", DEFAULT_DB_NAME),
        "username": secret_data.get("username", DEFAULT_DB_USER),
        "password": secret_data["password"],
    }
    return _cached_credentials


def get_connection() -> "psycopg2.extensions.connection":
    """Get a database connection."""
    creds = get_db_credentials()
    return psycopg2.connect(
        host=creds["host"],
        database=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Data generation helpers
# ---------------------------------------------------------------------------

def _generate_vin() -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(chars) for _ in range(17))


def _generate_plate() -> str:
    letters = "".join(secrets.choice(string.ascii_uppercase) for _ in range(3))
    return f"{letters}-{secrets.randbelow(9000) + 1000}"


def _generate_employee_id() -> str:
    """Generate a valid employee ID matching ^DRV\\d{5}$."""
    return f"DRV{secrets.randbelow(90000) + 10000}"


def _init_vehicle_state(zone: str = "manhattan") -> Dict[str, Any]:
    z = ZONES.get(zone, ZONES["manhattan"])
    return {
        "lat": rng.uniform(z["lat"][0], z["lat"][1]),
        "lon": rng.uniform(z["lon"][0], z["lon"][1]),
        "speed": rng.uniform(0, 40),
        "fuel": rng.uniform(50, 95),
        "odometer": rng.uniform(10000, 150000),
        "heading": rng.uniform(0, 360),
        "engine_temp": rng.uniform(80, 95),
        "zone": zone,
        "is_moving": rng.choice([True, True, True, False]),
    }


def _simulate_movement(state: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate realistic vehicle movement within NYC zones."""
    if not state["is_moving"]:
        if rng.random() < 0.2:
            state["is_moving"] = True
            state["speed"] = rng.uniform(5, 25)
        else:
            state["speed"] = 0
            return state

    if rng.random() < 0.1:
        state["is_moving"] = False
        state["speed"] = 0
        return state

    speed_delta = rng.uniform(-8, 12)
    state["speed"] = max(0, min(80, state["speed"] + speed_delta))

    if state["speed"] > 0:
        distance_km = state["speed"] / 3600
        if rng.random() < 0.15:
            state["heading"] = (state["heading"] + rng.choice([-90, -45, 45, 90])) % 360

        heading_rad = math.radians(state["heading"])
        state["lat"] += distance_km * 0.009 * math.cos(heading_rad)
        state["lon"] += distance_km * 0.012 * math.sin(heading_rad)

        z = ZONES.get(state["zone"], ZONES["manhattan"])
        state["lat"] = max(z["lat"][0], min(z["lat"][1], state["lat"]))
        state["lon"] = max(z["lon"][0], min(z["lon"][1], state["lon"]))

        state["odometer"] += distance_km
        state["fuel"] = max(5, state["fuel"] - 0.01 - (state["speed"] / 1000))
        state["engine_temp"] += rng.uniform(-0.5, 0.8)
        state["engine_temp"] = max(75, min(105, state["engine_temp"]))

    return state


# ---------------------------------------------------------------------------
# DQ violation generation (Task 10.2)
# ---------------------------------------------------------------------------

def _generate_dq_violation(
    record: Dict[str, Any],
    table: str,
) -> Tuple[Dict[str, Any], str]:
    """Inject a DQ violation into a record. Returns (modified_record, violation_type).

    Violations are chosen based on the table's configured DQ rules:
      - tel_001/002: out-of-range GPS
      - tel_003: speed > 350
      - tel_004: fuel > 120
      - tel_005: null vehicle_id
      - del_001: invalid delivery status
      - alt_001: invalid alert_type
      - drv_001: malformed employee_id
      - drv_002: null driver name
    """
    if table == "vehicle_telemetry":
        violation = rng.choice([
            "bad_lat", "bad_lon", "bad_speed", "bad_fuel", "null_vehicle_id",
        ])
        if violation == "bad_lat":
            record["latitude"] = rng.choice([
                rng.uniform(91, 200),
                rng.uniform(-200, -91),
            ])
        elif violation == "bad_lon":
            record["longitude"] = rng.choice([
                rng.uniform(181, 500),
                rng.uniform(-500, -181),
            ])
        elif violation == "bad_speed":
            record["speed_kmh"] = rng.uniform(351, 600)
        elif violation == "bad_fuel":
            record["fuel_level_pct"] = rng.uniform(121, 200)
        elif violation == "null_vehicle_id":
            record["vehicle_id"] = None
        return record, violation

    elif table == "deliveries":
        bad_statuses = ["CANCELLED", "unknown", "pending", "RETURNED", "lost"]
        record["status"] = rng.choice(bad_statuses)
        return record, "bad_delivery_status"

    elif table == "alerts":
        bad_types = ["tire_pressure", "battery_low", "door_open", "SPEEDING", "unknown_alert"]
        record["alert_type"] = rng.choice(bad_types)
        return record, "bad_alert_type"

    elif table == "drivers":
        violation = rng.choice(["bad_employee_id", "null_name"])
        if violation == "bad_employee_id":
            bad_ids = ["EMP001", "DRV1", "drv12345", "DRIVER00001", "12345"]
            record["employee_id"] = rng.choice(bad_ids)
        elif violation == "null_name":
            record["name"] = None
        return record, violation

    return record, "none"


# ---------------------------------------------------------------------------
# Seed actions (Task 10.1)
# ---------------------------------------------------------------------------

def seed_vehicles(conn: Any, count: int = 15) -> int:
    """Create initial vehicle reference data in RDS."""
    cur = conn.cursor()
    zones = list(ZONES.keys())
    created = 0
    for _ in range(count):
        make, model = rng.choice(MAKES_MODELS)
        zone = rng.choice(zones)
        try:
            cur.execute(
                """INSERT INTO vehicles (vin, license_plate, make, model, year, fuel_type, status)
                   VALUES (%s, %s, %s, %s, %s, %s, 'active') RETURNING id""",
                (_generate_vin(), _generate_plate(), make, model,
                 rng.randint(2018, 2025), rng.choice(FUEL_TYPES)),
            )
            vid = cur.fetchone()[0]
            VEHICLE_STATES[vid] = _init_vehicle_state(zone)
            created += 1
            conn.commit()
        except psycopg2.IntegrityError:
            conn.rollback()
    cur.close()
    return created


def seed_drivers(conn: Any, count: int = 10) -> int:
    """Create initial driver reference data in RDS."""
    cur = conn.cursor()
    created = 0
    for _ in range(count):
        name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        emp_id = _generate_employee_id()
        try:
            cur.execute(
                """INSERT INTO drivers (employee_id, name, license_number, phone, status, safety_score)
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
                (emp_id, name, f"DL{rng.randint(100000, 999999)}",
                 f"+1{rng.randint(2000000000, 9999999999)}",
                 rng.choice(["available", "available", "on_duty"]),
                 round(rng.uniform(70, 100), 1)),
            )
            created += 1
            conn.commit()
        except psycopg2.IntegrityError:
            conn.rollback()
    cur.close()
    return created


def _fetch_ids(conn: Any) -> Tuple[List[int], List[int]]:
    """Fetch active vehicle IDs and all driver IDs from the database."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM vehicles WHERE status = 'active'")
    vehicle_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM drivers WHERE status != 'terminated'")
    driver_ids = [r[0] for r in cur.fetchall()]
    cur.close()
    return vehicle_ids, driver_ids


# ---------------------------------------------------------------------------
# Telemetry generation
# ---------------------------------------------------------------------------

def _generate_telemetry_batch(
    conn: Any,
    vehicle_ids: List[int],
    stats: Dict[str, int],
    inject_dq: bool = True,
) -> None:
    """Generate one telemetry reading per active vehicle and INSERT into RDS."""
    cur = conn.cursor()
    now = utc_now()

    for vid in vehicle_ids:
        if vid not in VEHICLE_STATES:
            VEHICLE_STATES[vid] = _init_vehicle_state()

        state = _simulate_movement(VEHICLE_STATES[vid])
        harsh_brake = state["speed"] > 40 and rng.random() < 0.03
        harsh_accel = state["speed"] < 20 and state["is_moving"] and rng.random() < 0.03

        record: Dict[str, Any] = {
            "vehicle_id": vid,
            "latitude": round(state["lat"], 6),
            "longitude": round(state["lon"], 6),
            "speed_kmh": round(state["speed"], 1),
            "fuel_level_pct": round(state["fuel"], 2),
            "engine_temp_c": round(state["engine_temp"], 1),
            "odometer_km": round(state["odometer"], 1),
            "engine_status": "running" if state["is_moving"] else "idle",
            "harsh_braking": str(harsh_brake).lower(),
            "harsh_acceleration": str(harsh_accel).lower(),
        }

        # Inject DQ violation at configured rate
        if inject_dq and rng.random() < DQ_VIOLATION_RATE:
            record, vtype = _generate_dq_violation(record, "vehicle_telemetry")
            stats["dq_violations"] += 1

        cur.execute(
            """INSERT INTO vehicle_telemetry
               (vehicle_id, recorded_at, latitude, longitude, speed_kmh,
                fuel_level_pct, engine_temp_c, odometer_km, engine_status,
                harsh_braking, harsh_acceleration)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (record["vehicle_id"], now, record["latitude"], record["longitude"],
             record["speed_kmh"], record["fuel_level_pct"], record["engine_temp_c"],
             record["odometer_km"], record["engine_status"],
             record["harsh_braking"], record["harsh_acceleration"]),
        )
        stats["telemetry_inserted"] += 1

    conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
# Delivery generation
# ---------------------------------------------------------------------------

def _create_delivery(
    conn: Any,
    vehicle_ids: List[int],
    driver_ids: List[int],
    stats: Dict[str, int],
    inject_dq: bool = True,
) -> None:
    """Create a new delivery record."""
    cur = conn.cursor()
    vid = rng.choice(vehicle_ids)
    did = rng.choice(driver_ids)
    status = "assigned"

    record: Dict[str, Any] = {
        "vehicle_id": vid,
        "driver_id": did,
        "status": status,
        "pickup_address": f"{rng.randint(1, 999)} {rng.choice(STREETS)}, New York, NY",
        "delivery_address": f"{rng.randint(1, 999)} {rng.choice(STREETS)}, New York, NY",
        "scheduled_time": utc_now() + timedelta(hours=rng.randint(1, 6)),
        "customer_name": f"Customer {rng.randint(1000, 9999)}",
        "customer_phone": f"+1{rng.randint(2000000000, 9999999999)}",
    }

    if inject_dq and rng.random() < DQ_VIOLATION_RATE:
        record, _ = _generate_dq_violation(record, "deliveries")
        stats["dq_violations"] += 1

    cur.execute(
        """INSERT INTO deliveries
           (vehicle_id, driver_id, status, pickup_address, delivery_address,
            scheduled_time, customer_name, customer_phone)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
        (record["vehicle_id"], record["driver_id"], record["status"],
         record["pickup_address"], record["delivery_address"],
         record["scheduled_time"], record["customer_name"], record["customer_phone"]),
    )
    delivery_id = cur.fetchone()[0]
    ACTIVE_DELIVERIES[delivery_id] = {"status": record["status"], "created": utc_now()}
    conn.commit()
    cur.close()
    stats["deliveries_created"] += 1


def _update_deliveries(conn: Any, stats: Dict[str, int]) -> None:
    """Progress existing deliveries through their lifecycle."""
    cur = conn.cursor()
    transitions = [
        ("assigned", "picked_up", 0.3),
        ("picked_up", "in_transit", 0.4),
        ("in_transit", "delivered", 0.2),
        ("in_transit", "failed", 0.05),
    ]
    for old_status, new_status, probability in transitions:
        if rng.random() < probability:
            cur.execute(
                """UPDATE deliveries SET status = %s, updated_at = NOW()
                   WHERE id IN (
                       SELECT id FROM deliveries WHERE status = %s
                       ORDER BY created_at LIMIT 1
                       FOR UPDATE SKIP LOCKED
                   )""",
                (new_status, old_status),
            )
            stats["deliveries_updated"] += cur.rowcount
    conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
# Alert generation
# ---------------------------------------------------------------------------

def _generate_alerts(
    conn: Any,
    vehicle_ids: List[int],
    driver_ids: List[int],
    stats: Dict[str, int],
    inject_dq: bool = True,
) -> None:
    """Generate contextual alerts based on current vehicle states."""
    cur = conn.cursor()

    for vid in vehicle_ids:
        state = VEHICLE_STATES.get(vid)
        if not state:
            continue

        alert_type: Optional[str] = None
        severity: Optional[str] = None
        message: Optional[str] = None

        if state["speed"] > 70 and rng.random() < 0.3:
            alert_type = "speeding"
            severity = "warning"
            message = f"Vehicle exceeding speed limit: {state['speed']:.1f} km/h"
        elif state["fuel"] < 20 and rng.random() < 0.4:
            alert_type = "low_fuel"
            severity = "critical" if state["fuel"] < 10 else "warning"
            message = f"Low fuel warning: {state['fuel']:.1f}%"
        elif state["engine_temp"] > 100 and rng.random() < 0.5:
            alert_type = "engine_overheat"
            severity = "critical"
            message = f"Engine temperature high: {state['engine_temp']:.1f}C"
        elif rng.random() < 0.02:
            alert_type = "harsh_braking"
            severity = "info"
            message = "Harsh braking event detected"

        if alert_type is None:
            continue

        record: Dict[str, Any] = {
            "vehicle_id": vid,
            "driver_id": rng.choice(driver_ids),
            "alert_type": alert_type,
            "severity": severity,
            "message": message,
            "latitude": state["lat"],
            "longitude": state["lon"],
        }

        if inject_dq and rng.random() < DQ_VIOLATION_RATE:
            record, _ = _generate_dq_violation(record, "alerts")
            stats["dq_violations"] += 1

        cur.execute(
            """INSERT INTO alerts
               (vehicle_id, driver_id, alert_type, severity, message, latitude, longitude)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (record["vehicle_id"], record["driver_id"], record["alert_type"],
             record["severity"], record["message"], record["latitude"], record["longitude"]),
        )
        stats["alerts_created"] += 1

    conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
# SCD2 scenario generation (Task 10.3)
# ---------------------------------------------------------------------------

def _generate_scd2_vehicle_updates(conn: Any, stats: Dict[str, int]) -> None:
    """Generate UPDATE/DELETE operations on vehicles for SCD2 testing.

    - Status transitions: active -> maintenance -> decommissioned
    - Fuel type corrections
    - Soft-delete via status = 'decommissioned'
    """
    cur = conn.cursor()

    # UPDATE: status change (active -> maintenance)
    if rng.random() < 0.5:
        cur.execute(
            """UPDATE vehicles SET status = 'maintenance', updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM vehicles WHERE status = 'active'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
        )
        stats["updates"] += cur.rowcount

    # UPDATE: status change (maintenance -> active, back in service)
    if rng.random() < 0.3:
        cur.execute(
            """UPDATE vehicles SET status = 'active', updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM vehicles WHERE status = 'maintenance'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
        )
        stats["updates"] += cur.rowcount

    # UPDATE: fuel_type correction (simulates data correction)
    if rng.random() < 0.3:
        new_fuel = rng.choice(FUEL_TYPES)
        cur.execute(
            """UPDATE vehicles SET fuel_type = %s, updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM vehicles WHERE status = 'active'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
            (new_fuel,),
        )
        stats["updates"] += cur.rowcount

    # DELETE (soft): decommission a vehicle
    if rng.random() < 0.15:
        cur.execute(
            """UPDATE vehicles SET status = 'decommissioned', updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM vehicles WHERE status IN ('active', 'maintenance')
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )
               RETURNING id""",
        )
        row = cur.fetchone()
        if row:
            stats["deletes"] += 1
            # Remove from active tracking
            VEHICLE_STATES.pop(row[0], None)

    # Multiple updates on same PK within batch window
    if rng.random() < 0.2:
        cur.execute("SELECT id FROM vehicles WHERE status = 'active' ORDER BY RANDOM() LIMIT 1")
        row = cur.fetchone()
        if row:
            vid = row[0]
            # First update: change year
            cur.execute(
                "UPDATE vehicles SET year = %s, updated_at = NOW() WHERE id = %s",
                (rng.randint(2020, 2025), vid),
            )
            stats["updates"] += cur.rowcount
            # Second update on same PK: change license plate
            cur.execute(
                "UPDATE vehicles SET license_plate = %s, updated_at = NOW() WHERE id = %s",
                (_generate_plate(), vid),
            )
            stats["updates"] += cur.rowcount

    conn.commit()
    cur.close()


def _generate_scd2_driver_updates(conn: Any, stats: Dict[str, int]) -> None:
    """Generate UPDATE/DELETE operations on drivers for SCD2 testing.

    - Safety score changes
    - Status transitions: available -> on_duty -> suspended
    - Soft-delete via status = 'terminated'
    """
    cur = conn.cursor()

    # UPDATE: safety_score change
    if rng.random() < 0.5:
        delta = round(rng.uniform(-5, 5), 1)
        cur.execute(
            """UPDATE drivers SET
                   safety_score = GREATEST(0, LEAST(100, COALESCE(safety_score, 80) + %s)),
                   updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM drivers WHERE status != 'terminated'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
            (delta,),
        )
        stats["updates"] += cur.rowcount

    # UPDATE: status transition (available -> on_duty)
    if rng.random() < 0.4:
        cur.execute(
            """UPDATE drivers SET status = 'on_duty', updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM drivers WHERE status = 'available'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
        )
        stats["updates"] += cur.rowcount

    # UPDATE: status transition (on_duty -> available)
    if rng.random() < 0.3:
        cur.execute(
            """UPDATE drivers SET status = 'available', updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM drivers WHERE status = 'on_duty'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
        )
        stats["updates"] += cur.rowcount

    # UPDATE: suspend a driver (on_duty -> suspended)
    if rng.random() < 0.1:
        cur.execute(
            """UPDATE drivers SET status = 'suspended', updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM drivers WHERE status = 'on_duty'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
        )
        stats["updates"] += cur.rowcount

    # DELETE (soft): terminate a driver
    if rng.random() < 0.08:
        cur.execute(
            """UPDATE drivers SET status = 'terminated', updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM drivers WHERE status IN ('available', 'suspended')
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
        )
        stats["deletes"] += cur.rowcount

    # Multiple updates on same PK within batch window
    if rng.random() < 0.2:
        cur.execute(
            "SELECT id FROM drivers WHERE status != 'terminated' ORDER BY RANDOM() LIMIT 1"
        )
        row = cur.fetchone()
        if row:
            did = row[0]
            # First: update phone
            cur.execute(
                "UPDATE drivers SET phone = %s, updated_at = NOW() WHERE id = %s",
                (f"+1{rng.randint(2000000000, 9999999999)}", did),
            )
            stats["updates"] += cur.rowcount
            # Second: update safety_score on same PK
            cur.execute(
                "UPDATE drivers SET safety_score = %s, updated_at = NOW() WHERE id = %s",
                (round(rng.uniform(60, 100), 1), did),
            )
            stats["updates"] += cur.rowcount

    conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
# Glue DQ threshold trigger generation (Task 10.4)
# ---------------------------------------------------------------------------

def _generate_dq_trigger_batch(
    conn: Any,
    vehicle_ids: List[int],
    stats: Dict[str, int],
) -> None:
    """Generate a batch that intentionally triggers Glue DQ threshold warnings.

    Configured quality checks (DQDL) on vehicle_telemetry:
      - completeness(vehicle_id) >= 0.95  -> inject >5% null vehicle_id
      - completeness(latitude) >= 0.90    -> inject >10% null latitude
      - uniqueness(id) >= 1.0             -> inject duplicate telemetry IDs
      - size >= 1                          -> generate empty/tiny batch

    One trigger type is chosen per invocation.
    """
    trigger = rng.choice([
        "low_completeness_vehicle_id",
        "low_completeness_latitude",
        "duplicate_ids",
        "below_size",
    ])
    cur = conn.cursor()
    now = utc_now()

    if trigger == "low_completeness_vehicle_id":
        # >5% null vehicle_id in a batch of ~20 records
        batch_size = max(20, len(vehicle_ids))
        null_count = int(batch_size * 0.15)  # 15% nulls, well above 5% threshold
        for i in range(batch_size):
            vid = None if i < null_count else rng.choice(vehicle_ids)
            state = _init_vehicle_state()
            cur.execute(
                """INSERT INTO vehicle_telemetry
                   (vehicle_id, recorded_at, latitude, longitude, speed_kmh,
                    fuel_level_pct, engine_temp_c, odometer_km, engine_status,
                    harsh_braking, harsh_acceleration)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (vid, now, state["lat"], state["lon"],
                 round(state["speed"], 1), round(state["fuel"], 2),
                 round(state["engine_temp"], 1), round(state["odometer"], 1),
                 "idle", "false", "false"),
            )
            stats["telemetry_inserted"] += 1
            if vid is None:
                stats["dq_violations"] += 1

    elif trigger == "low_completeness_latitude":
        # >10% null latitude in a batch of ~20 records
        batch_size = max(20, len(vehicle_ids))
        null_count = int(batch_size * 0.25)  # 25% nulls, well above 10% threshold
        for i in range(batch_size):
            vid = rng.choice(vehicle_ids)
            lat = None if i < null_count else rng.uniform(40.60, 40.85)
            lon = None if i < null_count else rng.uniform(-74.05, -73.70)
            cur.execute(
                """INSERT INTO vehicle_telemetry
                   (vehicle_id, recorded_at, latitude, longitude, speed_kmh,
                    fuel_level_pct, engine_temp_c, odometer_km, engine_status,
                    harsh_braking, harsh_acceleration)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (vid, now, lat, lon,
                 round(rng.uniform(0, 60), 1), round(rng.uniform(30, 90), 2),
                 round(rng.uniform(80, 100), 1), round(rng.uniform(10000, 150000), 1),
                 "idle", "false", "false"),
            )
            stats["telemetry_inserted"] += 1

    elif trigger == "duplicate_ids":
        # Insert records, then re-insert some with the same auto-generated IDs
        # Since id is SERIAL, we read back recent IDs and insert with explicit id
        inserted_ids: List[int] = []
        for _ in range(10):
            vid = rng.choice(vehicle_ids)
            state = _init_vehicle_state()
            cur.execute(
                """INSERT INTO vehicle_telemetry
                   (vehicle_id, recorded_at, latitude, longitude, speed_kmh,
                    fuel_level_pct, engine_temp_c, odometer_km, engine_status,
                    harsh_braking, harsh_acceleration)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (vid, now, state["lat"], state["lon"],
                 round(state["speed"], 1), round(state["fuel"], 2),
                 round(state["engine_temp"], 1), round(state["odometer"], 1),
                 "running", "false", "false"),
            )
            row = cur.fetchone()
            if row:
                inserted_ids.append(row[0])
            stats["telemetry_inserted"] += 1

        # Insert duplicates by reusing the same vehicle_id + recorded_at combos
        # (The CDC stream will see these as separate inserts with duplicate logical keys)
        for _ in range(3):
            vid = rng.choice(vehicle_ids)
            state = _init_vehicle_state()
            cur.execute(
                """INSERT INTO vehicle_telemetry
                   (vehicle_id, recorded_at, latitude, longitude, speed_kmh,
                    fuel_level_pct, engine_temp_c, odometer_km, engine_status,
                    harsh_braking, harsh_acceleration)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (vid, now, state["lat"], state["lon"],
                 round(state["speed"], 1), round(state["fuel"], 2),
                 round(state["engine_temp"], 1), round(state["odometer"], 1),
                 "running", "false", "false"),
            )
            stats["telemetry_inserted"] += 1

    elif trigger == "below_size":
        # Generate an extremely small batch (0-1 records) to trigger size threshold
        if rng.random() < 0.5:
            # Completely empty — no insert at all
            pass
        else:
            vid = rng.choice(vehicle_ids)
            state = _init_vehicle_state()
            cur.execute(
                """INSERT INTO vehicle_telemetry
                   (vehicle_id, recorded_at, latitude, longitude, speed_kmh,
                    fuel_level_pct, engine_temp_c, odometer_km, engine_status,
                    harsh_braking, harsh_acceleration)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (vid, now, state["lat"], state["lon"],
                 round(state["speed"], 1), round(state["fuel"], 2),
                 round(state["engine_temp"], 1), round(state["odometer"], 1),
                 "idle", "false", "false"),
            )
            stats["telemetry_inserted"] += 1

    conn.commit()
    cur.close()
    stats["dq_trigger_batches"] += 1


# ---------------------------------------------------------------------------
# Stats helper
# ---------------------------------------------------------------------------

def _new_stats() -> Dict[str, int]:
    """Return a fresh stats dictionary."""
    return {
        "telemetry_inserted": 0,
        "dq_violations": 0,
        "updates": 0,
        "deletes": 0,
        "deliveries_created": 0,
        "deliveries_updated": 0,
        "alerts_created": 0,
        "dq_trigger_batches": 0,
    }


# ---------------------------------------------------------------------------
# Burst action (Task 10.1)
# ---------------------------------------------------------------------------

def _action_burst(
    conn: Any,
    vehicle_ids: List[int],
    driver_ids: List[int],
    total_records: int,
) -> Dict[str, int]:
    """Produce N records in a single batch across all 5 tables.

    Distribution: ~60% telemetry, ~15% deliveries, ~10% alerts,
                  ~10% SCD2 updates, ~5% SCD2 deletes.
    DQ violations injected at ~8% rate.
    """
    stats = _new_stats()

    # Calculate record counts per table
    n_telemetry = int(total_records * 0.60)
    n_deliveries = int(total_records * 0.15)
    n_alerts = int(total_records * 0.10)
    n_updates = int(total_records * 0.10)
    n_deletes = total_records - n_telemetry - n_deliveries - n_alerts - n_updates

    # Telemetry: generate in batches of len(vehicle_ids)
    telemetry_iterations = max(1, n_telemetry // max(1, len(vehicle_ids)))
    for _ in range(telemetry_iterations):
        _generate_telemetry_batch(conn, vehicle_ids, stats, inject_dq=True)

    # Deliveries
    for _ in range(n_deliveries):
        _create_delivery(conn, vehicle_ids, driver_ids, stats, inject_dq=True)

    # Alerts
    for _ in range(max(1, n_alerts // max(1, len(vehicle_ids)))):
        _generate_alerts(conn, vehicle_ids, driver_ids, stats, inject_dq=True)

    # SCD2 updates
    for _ in range(max(1, n_updates // 3)):
        _generate_scd2_vehicle_updates(conn, stats)
        _generate_scd2_driver_updates(conn, stats)

    # SCD2 deletes (already included in the update functions via probability)
    for _ in range(max(1, n_deletes)):
        if rng.random() < 0.5:
            _generate_scd2_vehicle_updates(conn, stats)
        else:
            _generate_scd2_driver_updates(conn, stats)

    return stats


# ---------------------------------------------------------------------------
# Generate action (Task 10.1)
# ---------------------------------------------------------------------------

def _action_generate(
    conn: Any,
    vehicle_ids: List[int],
    driver_ids: List[int],
    duration_seconds: int,
    interval_seconds: float,
) -> Dict[str, Any]:
    """Produce continuous data across all 5 tables for the given duration.

    Each iteration:
      1. Telemetry for all active vehicles (~8% bad data)
      2. Every 3rd iteration: create/update deliveries
      3. Every 4th iteration: generate contextual alerts
      4. Every 5th iteration: SCD2 updates on vehicles/drivers
      5. Every 10th iteration: DQ-trigger batch
    """
    stats = _new_stats()
    start = time.time()
    iteration = 0

    while time.time() - start < duration_seconds:
        # 1. Telemetry for all active vehicles
        _generate_telemetry_batch(conn, vehicle_ids, stats, inject_dq=True)

        # 2. Every 3rd iteration: deliveries
        if iteration % 3 == 0:
            if rng.random() < 0.6:
                _create_delivery(conn, vehicle_ids, driver_ids, stats, inject_dq=True)
            else:
                _update_deliveries(conn, stats)

        # 3. Every 4th iteration: alerts
        if iteration % 4 == 0:
            _generate_alerts(conn, vehicle_ids, driver_ids, stats, inject_dq=True)

        # 4. Every 5th iteration: SCD2 updates/deletes
        if iteration % 5 == 0:
            _generate_scd2_vehicle_updates(conn, stats)
            _generate_scd2_driver_updates(conn, stats)

        # 5. Every 10th iteration: DQ trigger batch
        if iteration % 10 == 0 and iteration > 0:
            _generate_dq_trigger_batch(conn, vehicle_ids, stats)

        # Refresh vehicle/driver IDs periodically (SCD2 may decommission/terminate)
        if iteration % 20 == 0 and iteration > 0:
            new_vids, new_dids = _fetch_ids(conn)
            if new_vids:
                vehicle_ids = new_vids
            if new_dids:
                driver_ids = new_dids

        iteration += 1
        time.sleep(interval_seconds)

    result: Dict[str, Any] = dict(stats)
    result["duration_seconds"] = round(time.time() - start, 1)
    result["iterations"] = iteration
    result["vehicles_active"] = len(vehicle_ids)
    return result


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entry point.

    Actions:
      {"action": "seed", "vehicles": 15, "drivers": 10}
      {"action": "generate", "duration_seconds": 300, "interval_seconds": 2}
      {"action": "burst", "records": 200}
    """
    action = event.get("action", "generate")
    conn = get_connection()
    results: Dict[str, Any] = {"action": action}

    try:
        if action == "seed":
            vehicle_count = event.get("vehicles", 15)
            driver_count = event.get("drivers", 10)
            results["vehicles_created"] = seed_vehicles(conn, vehicle_count)
            results["drivers_created"] = seed_drivers(conn, driver_count)

        elif action == "burst":
            record_count = event.get("records", 200)
            vehicle_ids, driver_ids = _fetch_ids(conn)
            if not vehicle_ids or not driver_ids:
                return {
                    "statusCode": 400,
                    "body": "No vehicles/drivers found. Run action=seed first.",
                }
            stats = _action_burst(conn, vehicle_ids, driver_ids, record_count)
            results.update(stats)

        elif action == "generate":
            duration = event.get("duration_seconds", 60)
            interval = event.get("interval_seconds", 2)
            vehicle_ids, driver_ids = _fetch_ids(conn)
            if not vehicle_ids or not driver_ids:
                return {
                    "statusCode": 400,
                    "body": "No vehicles/drivers found. Run action=seed first.",
                }
            gen_results = _action_generate(
                conn, vehicle_ids, driver_ids, duration, interval,
            )
            results.update(gen_results)

        else:
            return {
                "statusCode": 400,
                "body": f"Unknown action: {action}. Use seed, generate, or burst.",
            }

    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
    finally:
        conn.close()

    return {"statusCode": 200, "body": json.dumps(results, default=str)}
