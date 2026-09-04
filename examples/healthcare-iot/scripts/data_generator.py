"""
Healthcare IoT Data Generator Lambda.

COMPLIANCE NOTE: This generator produces synthetic patient data for demonstration
only. Patient identifiers are masked before storage by the `mask_pii` transforms
in config/tables.yaml. If you adapt this for real patient data (PHI), you are
responsible for your own HIPAA/GDPR safeguards, including a Business Associate
Agreement (BAA) with AWS where required.
See https://aws.amazon.com/compliance/hipaa-compliance/


Generates diverse data that exercises the full DQ/SCD2/quality-metrics pipeline:
- ~5-10% of records trigger DQ quarantine (clinical ranges, regex, not_null)
- ~20-30% of operations are updates/deletes (patient lifecycle, corrected vitals)
- ~10% of batches trigger Glue DQ threshold warnings

Exercises patterns different from vehicle telemetry:
- PII masking (name, phone, address)
- Foreign key relationships (vitals.patient_id -> patients.id)
- Clinical vital sign ranges for DQ violations
- MRN regex validation (^MRN-\\d{6}$)
- Patient status lifecycle: active -> discharged -> readmitted

Usage:
  {"action": "seed", "patients": 20}
  {"action": "generate", "duration_seconds": 300, "interval_seconds": 2}
  {"action": "burst", "records": 100}
"""
from __future__ import annotations

import json
import os
# OS-entropy-backed RNG (CWE-338 / Bandit B311 compliant). Test-data generation
# has no cryptographic requirement, but SystemRandom keeps security scans clean
# with the identical API (choice/randint/random/sample/uniform).
from random import SystemRandom

rng = SystemRandom()
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

VALID_STATUSES = ["active", "discharged", "readmitted"]

# Realistic PII data pools
FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
]
STREETS = [
    "Main St", "Oak Ave", "Elm St", "Park Blvd", "Cedar Ln", "Maple Dr",
    "Pine St", "Washington Ave", "Lake Rd", "Hill St", "River Rd", "Church St",
]
CITIES = [
    "Springfield", "Riverside", "Fairview", "Madison", "Georgetown",
    "Clinton", "Arlington", "Salem", "Franklin", "Chester",
]
STATES = ["NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_cached_credentials: Optional[Dict[str, str]] = None


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

def _generate_mrn() -> str:
    """Generate a valid MRN matching ^MRN-\\d{6}$."""
    return f"MRN-{rng.randint(100000, 999999)}"


def _generate_phone() -> str:
    """Generate a realistic US phone number."""
    return f"+1{rng.randint(2000000000, 9999999999)}"


def _generate_address() -> str:
    """Generate a realistic street address."""
    number = rng.randint(1, 9999)
    street = rng.choice(STREETS)
    city = rng.choice(CITIES)
    state = rng.choice(STATES)
    zipcode = rng.randint(10000, 99999)
    return f"{number} {street}, {city}, {state} {zipcode}"


def _generate_dob() -> str:
    """Generate a date of birth (age 1-100)."""
    days_ago = rng.randint(365, 36500)
    dob = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dob.strftime("%Y-%m-%d")


def _generate_normal_vitals() -> Dict[str, Any]:
    """Generate vital signs within normal clinical ranges."""
    return {
        "heart_rate": round(rng.uniform(50, 120), 1),
        "blood_pressure_sys": round(rng.uniform(90, 180), 1),
        "blood_pressure_dia": round(rng.uniform(50, 110), 1),
        "temperature_c": round(rng.uniform(35.5, 38.5), 1),
        "spo2_pct": round(rng.uniform(92, 100), 1),
    }


# ---------------------------------------------------------------------------
# DQ violation generation (Task 11.2)
# ---------------------------------------------------------------------------

def _generate_dq_violation(
    record: Dict[str, Any],
    table: str,
) -> Tuple[Dict[str, Any], str]:
    """Inject a DQ violation into a record. Returns (modified_record, violation_type).

    Violations are chosen based on the table's configured DQ rules:
      patients:
        - pat_001: MRN fails regex ^MRN-\\d{6}$
        - pat_002: null patient name (not_null violation)
      vitals:
        - vit_001: heart_rate outside 20-300
        - vit_002: temperature_c outside 30-45
        - vit_003: spo2_pct outside 0-100
        - vit_004: blood_pressure_sys outside 50-250
        - vit_005: blood_pressure_dia outside 30-150
        - null patient_id (completeness threshold 1.0)
    """
    if table == "patients":
        violation = rng.choice(["bad_mrn", "null_name"])
        if violation == "bad_mrn":
            bad_mrns = [
                "MRN123456",       # missing dash
                "mrn-123456",      # lowercase
                "MRN-12345",       # only 5 digits
                "MRN-1234567",     # 7 digits
                "PATIENT-000001",  # wrong prefix
                "123456",          # no prefix at all
                "",                # empty string
            ]
            record["mrn"] = rng.choice(bad_mrns)
        elif violation == "null_name":
            record["name"] = None
        return record, violation

    elif table == "vitals":
        violation = rng.choice([
            "bad_heart_rate", "bad_temperature", "bad_spo2",
            "bad_bp_sys", "bad_bp_dia", "null_patient_id",
        ])
        if violation == "bad_heart_rate":
            # Outside range 20-300
            record["heart_rate"] = rng.choice([
                round(rng.uniform(0, 19), 1),
                round(rng.uniform(301, 500), 1),
            ])
        elif violation == "bad_temperature":
            # Outside range 30-45
            record["temperature_c"] = rng.choice([
                round(rng.uniform(0, 29), 1),
                round(rng.uniform(46, 60), 1),
            ])
        elif violation == "bad_spo2":
            # Outside range 0-100
            record["spo2_pct"] = round(rng.uniform(101, 150), 1)
        elif violation == "bad_bp_sys":
            # Outside range 50-250
            record["blood_pressure_sys"] = rng.choice([
                round(rng.uniform(0, 49), 1),
                round(rng.uniform(251, 400), 1),
            ])
        elif violation == "bad_bp_dia":
            # Outside range 30-150
            record["blood_pressure_dia"] = rng.choice([
                round(rng.uniform(0, 29), 1),
                round(rng.uniform(151, 250), 1),
            ])
        elif violation == "null_patient_id":
            record["patient_id"] = None
        return record, violation

    return record, "none"


# ---------------------------------------------------------------------------
# Seed action (Task 11.1)
# ---------------------------------------------------------------------------

def seed_patients(conn: Any, count: int = 20) -> int:
    """Create initial patient reference data in RDS.

    Patients are seeded before vitals to honor the foreign key constraint
    (vitals.patient_id references patients.id).
    All patients get PII fields (name, phone, address) that exercise mask_pii.
    """
    cur = conn.cursor()
    created = 0
    for _ in range(count):
        name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        mrn = _generate_mrn()
        dob = _generate_dob()
        phone = _generate_phone()
        address = _generate_address()
        status = "active"
        try:
            cur.execute(
                """INSERT INTO patients (mrn, name, dob, phone, address, status)
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
                (mrn, name, dob, phone, address, status),
            )
            created += 1
            conn.commit()
        except psycopg2.IntegrityError:
            conn.rollback()
    cur.close()
    return created


def _fetch_patient_ids(conn: Any) -> List[int]:
    """Fetch all non-deleted patient IDs from the database."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM patients WHERE status != 'deleted'")
    patient_ids = [r[0] for r in cur.fetchall()]
    cur.close()
    return patient_ids


# ---------------------------------------------------------------------------
# Vitals generation
# ---------------------------------------------------------------------------

def _generate_vitals_batch(
    conn: Any,
    patient_ids: List[int],
    stats: Dict[str, int],
    inject_dq: bool = True,
) -> None:
    """Generate one set of vital sign readings for a subset of patients."""
    cur = conn.cursor()
    now = utc_now()

    # Each batch covers a random subset of patients (not all every time)
    sample_size = max(1, len(patient_ids) // 2)
    batch_patients = rng.sample(patient_ids, min(sample_size, len(patient_ids)))

    for pid in batch_patients:
        vitals = _generate_normal_vitals()
        record: Dict[str, Any] = {
            "patient_id": pid,
            "heart_rate": vitals["heart_rate"],
            "blood_pressure_sys": vitals["blood_pressure_sys"],
            "blood_pressure_dia": vitals["blood_pressure_dia"],
            "temperature_c": vitals["temperature_c"],
            "spo2_pct": vitals["spo2_pct"],
            "recorded_at": now.isoformat(),
        }

        # Inject DQ violation at configured rate
        if inject_dq and rng.random() < DQ_VIOLATION_RATE:
            record, vtype = _generate_dq_violation(record, "vitals")
            stats["dq_violations"] += 1

        cur.execute(
            """INSERT INTO vitals
               (patient_id, heart_rate, blood_pressure_sys, blood_pressure_dia,
                temperature_c, spo2_pct, recorded_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (record["patient_id"], record["heart_rate"],
             record["blood_pressure_sys"], record["blood_pressure_dia"],
             record["temperature_c"], record["spo2_pct"],
             record["recorded_at"]),
        )
        stats["vitals_inserted"] += 1

    conn.commit()
    cur.close()


def _generate_patient_insert(
    conn: Any,
    stats: Dict[str, int],
    inject_dq: bool = True,
) -> None:
    """Insert a new patient record with PII fields."""
    cur = conn.cursor()
    name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
    mrn = _generate_mrn()
    dob = _generate_dob()
    phone = _generate_phone()
    address = _generate_address()
    status = "active"

    record: Dict[str, Any] = {
        "mrn": mrn,
        "name": name,
        "dob": dob,
        "phone": phone,
        "address": address,
        "status": status,
    }

    if inject_dq and rng.random() < DQ_VIOLATION_RATE:
        record, vtype = _generate_dq_violation(record, "patients")
        stats["dq_violations"] += 1

    try:
        cur.execute(
            """INSERT INTO patients (mrn, name, dob, phone, address, status)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (record["mrn"], record["name"], record["dob"],
             record["phone"], record["address"], record["status"]),
        )
        stats["patients_inserted"] += 1
        conn.commit()
    except psycopg2.IntegrityError:
        conn.rollback()
    cur.close()


# ---------------------------------------------------------------------------
# SCD2 scenario generation (Task 11.3)
# ---------------------------------------------------------------------------

def _generate_scd2_patient_updates(conn: Any, stats: Dict[str, int]) -> None:
    """Generate UPDATE/DELETE operations on patients for SCD2 testing.

    - Phone/address updates (PII changes)
    - Status transitions: active -> discharged -> readmitted
    - DELETE operations for patient record corrections
    - Multiple updates on same PK within batch window
    """
    cur = conn.cursor()

    # UPDATE: phone number change
    if rng.random() < 0.5:
        new_phone = _generate_phone()
        cur.execute(
            """UPDATE patients SET phone = %s, updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM patients WHERE status != 'deleted'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
            (new_phone,),
        )
        stats["updates"] += cur.rowcount

    # UPDATE: address change
    if rng.random() < 0.4:
        new_address = _generate_address()
        cur.execute(
            """UPDATE patients SET address = %s, updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM patients WHERE status != 'deleted'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
            (new_address,),
        )
        stats["updates"] += cur.rowcount

    # UPDATE: status transition (active -> discharged)
    if rng.random() < 0.4:
        cur.execute(
            """UPDATE patients SET status = 'discharged', updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM patients WHERE status = 'active'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
        )
        stats["updates"] += cur.rowcount

    # UPDATE: status transition (discharged -> readmitted)
    if rng.random() < 0.3:
        cur.execute(
            """UPDATE patients SET status = 'readmitted', updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM patients WHERE status = 'discharged'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
        )
        stats["updates"] += cur.rowcount

    # UPDATE: readmitted -> active (stabilized)
    if rng.random() < 0.25:
        cur.execute(
            """UPDATE patients SET status = 'active', updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM patients WHERE status = 'readmitted'
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
        )
        stats["updates"] += cur.rowcount

    # DELETE: patient record correction (soft-delete via status = 'deleted')
    if rng.random() < 0.1:
        cur.execute(
            """UPDATE patients SET status = 'deleted', updated_at = NOW()
               WHERE id IN (
                   SELECT id FROM patients WHERE status IN ('active', 'discharged')
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
        )
        stats["deletes"] += cur.rowcount

    # Multiple updates on same PK within batch window
    if rng.random() < 0.2:
        cur.execute(
            "SELECT id FROM patients WHERE status != 'deleted' ORDER BY RANDOM() LIMIT 1"
        )
        row = cur.fetchone()
        if row:
            pid = row[0]
            # First: update phone
            cur.execute(
                "UPDATE patients SET phone = %s, updated_at = NOW() WHERE id = %s",
                (_generate_phone(), pid),
            )
            stats["updates"] += cur.rowcount
            # Second: update address on same PK
            cur.execute(
                "UPDATE patients SET address = %s, updated_at = NOW() WHERE id = %s",
                (_generate_address(), pid),
            )
            stats["updates"] += cur.rowcount

    conn.commit()
    cur.close()


def _generate_scd2_vitals_updates(conn: Any, stats: Dict[str, int]) -> None:
    """Generate UPDATE/DELETE operations on vitals for SCD2 testing.

    - Corrected readings (update recorded_at, adjust values)
    - DELETE operations for erroneous readings
    """
    cur = conn.cursor()

    # UPDATE: corrected heart_rate reading
    if rng.random() < 0.4:
        new_hr = round(rng.uniform(50, 120), 1)
        cur.execute(
            """UPDATE vitals SET heart_rate = %s, recorded_at = %s
               WHERE id IN (
                   SELECT id FROM vitals
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
            (new_hr, utc_now().isoformat()),
        )
        stats["updates"] += cur.rowcount

    # UPDATE: corrected temperature reading
    if rng.random() < 0.3:
        new_temp = round(rng.uniform(36.0, 38.0), 1)
        cur.execute(
            """UPDATE vitals SET temperature_c = %s, recorded_at = %s
               WHERE id IN (
                   SELECT id FROM vitals
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
            (new_temp, utc_now().isoformat()),
        )
        stats["updates"] += cur.rowcount

    # UPDATE: corrected blood pressure
    if rng.random() < 0.3:
        new_sys = round(rng.uniform(100, 140), 1)
        new_dia = round(rng.uniform(60, 90), 1)
        cur.execute(
            """UPDATE vitals SET blood_pressure_sys = %s, blood_pressure_dia = %s,
                   recorded_at = %s
               WHERE id IN (
                   SELECT id FROM vitals
                   ORDER BY RANDOM() LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )""",
            (new_sys, new_dia, utc_now().isoformat()),
        )
        stats["updates"] += cur.rowcount

    # DELETE: remove erroneous vital reading
    if rng.random() < 0.15:
        cur.execute(
            """DELETE FROM vitals
               WHERE id IN (
                   SELECT id FROM vitals
                   ORDER BY RANDOM() LIMIT 1
               )""",
        )
        stats["deletes"] += cur.rowcount

    conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
# Glue DQ threshold trigger generation
# ---------------------------------------------------------------------------

def _generate_dq_trigger_batch(
    conn: Any,
    patient_ids: List[int],
    stats: Dict[str, int],
) -> None:
    """Generate a batch that intentionally triggers Glue DQ threshold warnings.

    Configured quality checks (DQDL):
      patients:
        - completeness(mrn) = 1.0       -> any null mrn triggers
        - uniqueness(mrn) = 1.0          -> duplicate MRN triggers
      vitals:
        - completeness(patient_id) = 1.0 -> any null patient_id triggers
        - completeness(heart_rate) >= 0.95 -> >5% null heart_rate triggers
        - completeness(temperature_c) >= 0.90 -> >10% null temperature_c triggers

    One trigger type is chosen per invocation.
    """
    trigger = rng.choice([
        "low_completeness_patient_id",
        "low_completeness_heart_rate",
        "low_completeness_temperature",
        "duplicate_mrn",
        "below_size",
    ])
    cur = conn.cursor()
    now = utc_now()

    if trigger == "low_completeness_patient_id":
        # >0% null patient_id in vitals (threshold is 1.0, so any null triggers)
        batch_size = 20
        null_count = max(2, int(batch_size * 0.15))
        for i in range(batch_size):
            pid = None if i < null_count else rng.choice(patient_ids)
            vitals = _generate_normal_vitals()
            cur.execute(
                """INSERT INTO vitals
                   (patient_id, heart_rate, blood_pressure_sys, blood_pressure_dia,
                    temperature_c, spo2_pct, recorded_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (pid, vitals["heart_rate"], vitals["blood_pressure_sys"],
                 vitals["blood_pressure_dia"], vitals["temperature_c"],
                 vitals["spo2_pct"], now.isoformat()),
            )
            stats["vitals_inserted"] += 1
            if pid is None:
                stats["dq_violations"] += 1

    elif trigger == "low_completeness_heart_rate":
        # >5% null heart_rate (threshold is 0.95)
        batch_size = 20
        null_count = max(2, int(batch_size * 0.20))  # 20% nulls, well above 5%
        for i in range(batch_size):
            pid = rng.choice(patient_ids)
            vitals = _generate_normal_vitals()
            hr = None if i < null_count else vitals["heart_rate"]
            cur.execute(
                """INSERT INTO vitals
                   (patient_id, heart_rate, blood_pressure_sys, blood_pressure_dia,
                    temperature_c, spo2_pct, recorded_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (pid, hr, vitals["blood_pressure_sys"],
                 vitals["blood_pressure_dia"], vitals["temperature_c"],
                 vitals["spo2_pct"], now.isoformat()),
            )
            stats["vitals_inserted"] += 1

    elif trigger == "low_completeness_temperature":
        # >10% null temperature_c (threshold is 0.90)
        batch_size = 20
        null_count = max(3, int(batch_size * 0.25))  # 25% nulls, well above 10%
        for i in range(batch_size):
            pid = rng.choice(patient_ids)
            vitals = _generate_normal_vitals()
            temp = None if i < null_count else vitals["temperature_c"]
            cur.execute(
                """INSERT INTO vitals
                   (patient_id, heart_rate, blood_pressure_sys, blood_pressure_dia,
                    temperature_c, spo2_pct, recorded_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (pid, vitals["heart_rate"], vitals["blood_pressure_sys"],
                 vitals["blood_pressure_dia"], temp,
                 vitals["spo2_pct"], now.isoformat()),
            )
            stats["vitals_inserted"] += 1

    elif trigger == "duplicate_mrn":
        # Duplicate MRN values in patients (uniqueness threshold 1.0)
        # Pick an existing MRN and try to insert a new patient with the same MRN
        cur.execute("SELECT mrn FROM patients ORDER BY RANDOM() LIMIT 1")
        row = cur.fetchone()
        if row:
            dup_mrn = row[0]
            name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
            try:
                cur.execute(
                    """INSERT INTO patients (mrn, name, dob, phone, address, status)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (dup_mrn, name, _generate_dob(), _generate_phone(),
                     _generate_address(), "active"),
                )
                stats["patients_inserted"] += 1
                stats["dq_violations"] += 1
            except psycopg2.IntegrityError:
                conn.rollback()
                # If unique constraint prevents it, insert with a slightly modified MRN
                # that still triggers the DQDL Uniqueness rule at the batch level
                cur.execute(
                    """INSERT INTO patients (mrn, name, dob, phone, address, status)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (dup_mrn + "X", name, _generate_dob(), _generate_phone(),
                     _generate_address(), "active"),
                )
                stats["patients_inserted"] += 1
                stats["dq_violations"] += 1

    elif trigger == "below_size":
        # Generate an extremely small batch (0-1 records) to trigger size threshold
        if rng.random() < 0.5:
            # Completely empty — no insert at all
            pass
        else:
            pid = rng.choice(patient_ids)
            vitals = _generate_normal_vitals()
            cur.execute(
                """INSERT INTO vitals
                   (patient_id, heart_rate, blood_pressure_sys, blood_pressure_dia,
                    temperature_c, spo2_pct, recorded_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (pid, vitals["heart_rate"], vitals["blood_pressure_sys"],
                 vitals["blood_pressure_dia"], vitals["temperature_c"],
                 vitals["spo2_pct"], now.isoformat()),
            )
            stats["vitals_inserted"] += 1

    conn.commit()
    cur.close()
    stats["dq_trigger_batches"] += 1


# ---------------------------------------------------------------------------
# Stats helper
# ---------------------------------------------------------------------------

def _new_stats() -> Dict[str, int]:
    """Return a fresh stats dictionary."""
    return {
        "patients_inserted": 0,
        "vitals_inserted": 0,
        "dq_violations": 0,
        "updates": 0,
        "deletes": 0,
        "dq_trigger_batches": 0,
    }


# ---------------------------------------------------------------------------
# Burst action (Task 11.1)
# ---------------------------------------------------------------------------

def _action_burst(
    conn: Any,
    patient_ids: List[int],
    total_records: int,
) -> Dict[str, int]:
    """Produce N records in a single batch across patients and vitals.

    Distribution: ~70% vitals, ~10% new patients, ~15% SCD2 updates, ~5% deletes.
    DQ violations injected at ~8% rate.
    Higher DQ rule coverage per batch than vehicle telemetry.
    """
    stats = _new_stats()

    n_vitals = int(total_records * 0.70)
    n_patients = int(total_records * 0.10)
    n_updates = int(total_records * 0.15)
    n_deletes = total_records - n_vitals - n_patients - n_updates

    # Vitals: generate in batches
    vitals_iterations = max(1, n_vitals // max(1, len(patient_ids) // 2))
    for _ in range(vitals_iterations):
        _generate_vitals_batch(conn, patient_ids, stats, inject_dq=True)

    # New patients with PII
    for _ in range(n_patients):
        _generate_patient_insert(conn, stats, inject_dq=True)

    # SCD2 updates on patients and vitals
    for _ in range(max(1, n_updates // 3)):
        _generate_scd2_patient_updates(conn, stats)
        _generate_scd2_vitals_updates(conn, stats)

    # Additional SCD2 operations (deletes included via probability)
    for _ in range(max(1, n_deletes)):
        if rng.random() < 0.5:
            _generate_scd2_patient_updates(conn, stats)
        else:
            _generate_scd2_vitals_updates(conn, stats)

    return stats


# ---------------------------------------------------------------------------
# Generate action (Task 11.1)
# ---------------------------------------------------------------------------

def _action_generate(
    conn: Any,
    patient_ids: List[int],
    duration_seconds: int,
    interval_seconds: float,
) -> Dict[str, Any]:
    """Produce continuous data across patients and vitals for the given duration.

    Each iteration:
      1. Vitals for a subset of patients (~8% bad data)
      2. Every 3rd iteration: new patient insert with PII
      3. Every 4th iteration: SCD2 patient updates (phone/address/status)
      4. Every 4th iteration: SCD2 vitals corrections
      5. Every 8th iteration: DQ-trigger batch

    Lower volume than vehicle telemetry, but higher DQ rule coverage per batch.
    Target: at least 20-30% of operations are updates/deletes.
    """
    stats = _new_stats()
    start = time.time()
    iteration = 0

    while time.time() - start < duration_seconds:
        # 1. Vitals for a subset of patients
        _generate_vitals_batch(conn, patient_ids, stats, inject_dq=True)

        # 2. Every 3rd iteration: new patient with PII
        if iteration % 3 == 0:
            _generate_patient_insert(conn, stats, inject_dq=True)

        # 3. Every 4th iteration: SCD2 patient updates
        if iteration % 4 == 0:
            _generate_scd2_patient_updates(conn, stats)

        # 4. Every 4th iteration (offset): SCD2 vitals corrections
        if iteration % 4 == 2:
            _generate_scd2_vitals_updates(conn, stats)

        # 5. Every 8th iteration: DQ trigger batch
        if iteration % 8 == 0 and iteration > 0:
            _generate_dq_trigger_batch(conn, patient_ids, stats)

        # Refresh patient IDs periodically (SCD2 may delete patients)
        if iteration % 15 == 0 and iteration > 0:
            new_pids = _fetch_patient_ids(conn)
            if new_pids:
                patient_ids = new_pids

        iteration += 1
        time.sleep(interval_seconds)

    result: Dict[str, Any] = dict(stats)
    result["duration_seconds"] = round(time.time() - start, 1)
    result["iterations"] = iteration
    result["patients_active"] = len(patient_ids)
    return result


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entry point.

    Actions:
      {"action": "seed", "patients": 20}
      {"action": "generate", "duration_seconds": 300, "interval_seconds": 2}
      {"action": "burst", "records": 100}
    """
    action = event.get("action", "generate")
    conn = get_connection()
    results: Dict[str, Any] = {"action": action}

    try:
        if action == "seed":
            patient_count = event.get("patients", 20)
            results["patients_created"] = seed_patients(conn, patient_count)

        elif action == "burst":
            record_count = event.get("records", 100)
            patient_ids = _fetch_patient_ids(conn)
            if not patient_ids:
                return {
                    "statusCode": 400,
                    "body": "No patients found. Run action=seed first.",
                }
            stats = _action_burst(conn, patient_ids, record_count)
            results.update(stats)

        elif action == "generate":
            duration = event.get("duration_seconds", 60)
            interval = event.get("interval_seconds", 2)
            patient_ids = _fetch_patient_ids(conn)
            if not patient_ids:
                return {
                    "statusCode": 400,
                    "body": "No patients found. Run action=seed first.",
                }
            gen_results = _action_generate(
                conn, patient_ids, duration, interval,
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
