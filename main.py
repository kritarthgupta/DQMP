from datetime import datetime
from engine import (
    get_connection,
    fetch_all,
    execute,
    volume_check,
    null_check,
    freshness_check,
    duplicate_check,
    referential_check,
    schema_drift_check
) 

def main():
    print("🚀 Starting DQMP run...")

    run_id = datetime.now()
    run_time = datetime.now()

    conn = get_connection()

    # -----------------------------
    # Fetch metadata
    # -----------------------------
    metadata = fetch_all("SELECT * FROM table_metadata")

    # Group metadata by table
    tables = {}
    for row in metadata:
        tables.setdefault(row["table_name"], []).append(row)

    total_checks = 0
    total_failures = 0

    # -----------------------------
    # Run DQ checks
    # -----------------------------
    for table_name, cols in tables.items():
        print(f"\n🔍 Checking table: {table_name}")

        # -------- Volume Check --------
        # Pick the first column that has volume thresholds
        meta = next(col for col in cols if col["expected_volume_min"] is not None)
        vol, threshold, status = volume_check(
            table_name,
            meta["expected_volume_min"],
            meta["expected_volume_max"],
            conn
        )
        # Insert Volume result
        execute("""
            INSERT INTO dq_results
            (run_id, run_time, table_name, column_name, metric_type, metric_value, threshold_value, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, run_time, table_name, None, "volume", vol, threshold, status))

        total_checks += 1
        total_failures += (status == "FAIL")


        # -------- Schema Drift Check --------
        drift, threshold, status, details = schema_drift_check(
            table_name,
            conn
        )

        # Insert Schema Drift result 
        execute("""
        INSERT INTO dq_results
        (run_id, run_time, table_name, column_name, metric_type, metric_value, threshold_value, status, details)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, run_time, table_name, None,
            "schema_drift", drift, threshold, status, details))

        total_checks += 1
        total_failures += (status == "FAIL")

        # -------- Column-level Checks --------
        for col in cols:
            column_name = col["column_name"]

            # Null check
            if column_name:
                null_pct, threshold, status = null_check(
                    table_name,
                    column_name,
                    col["is_nullable"],
                    conn
                )
                # Insert Null result
                execute("""
                    INSERT INTO dq_results
                    (run_id, run_time, table_name, column_name, metric_type, metric_value, threshold_value, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (run_id, run_time, table_name, column_name,
                      "null_pct", null_pct, threshold, status))

                total_checks += 1
                total_failures += (status == "FAIL")

            # Duplicate check for primary keys
            if col.get("is_primary_key") == 1:

                dup_count, threshold, status = duplicate_check(
                    table_name,
                    column_name,
                    conn
                )

                execute("""
                    INSERT INTO dq_results
                    (run_id, run_time, table_name, column_name, metric_type, metric_value, threshold_value, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (run_id, run_time, table_name, column_name,
                    "duplicate_count", dup_count, threshold, status))

                total_checks += 1
                total_failures += (status == "FAIL")

            # Referential integrity check
            if col.get("reference_table"):

                orphan_count, threshold, status = referential_check(
                    table_name,
                    column_name,
                    col["reference_table"],
                    col["reference_column"],
                    conn
                )

                execute("""
                    INSERT INTO dq_results
                    (run_id, run_time, table_name, column_name, metric_type, metric_value, threshold_value, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (run_id, run_time, table_name, column_name,
                    "orphan_records", orphan_count, threshold, status))

                total_checks += 1
                total_failures += (status == "FAIL")

            # Freshness check
            if col["freshness_sla_minutes"] is not None:
                freshness, threshold, status = freshness_check(
                    table_name,
                    column_name,
                    col["freshness_sla_minutes"],
                    conn
                )
            else:
                freshness, threshold, status = None, None, "SKIP"

            # Insert Freshness result
            execute("""
                INSERT INTO dq_results
                (run_id, run_time, table_name, column_name, metric_type, metric_value, threshold_value, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (run_id, run_time, table_name, column_name,
                  "freshness_minutes", freshness, threshold, status))

            total_checks += 1
            total_failures += (status == "FAIL")

    # -----------------------------
    # Generate alerts
    # -----------------------------
    failures = fetch_all("""
        SELECT * FROM dq_results
        WHERE run_id = (SELECT MAX(run_id) FROM dq_results) 
          AND status = 'FAIL'
    """)

    for row in failures:
        metric = row["metric_type"]

        severity = (
            "CRITICAL" if metric in ("volume", "freshness_minutes")
            else "WARNING"
        )

        message = f"DQ check failed for {row['table_name']}"
        if row["column_name"]:
            message += f".{row['column_name']}"
        message += f" on {metric}"
        # Add schema drift details if present
        if row.get("details"):
            message += f" | {row['details']}"

        execute("""
            INSERT INTO dq_alerts
            (alert_time, run_id, table_name, column_name, metric_type, severity, message)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (datetime.now(), run_id, row["table_name"],
              row["column_name"], metric, severity, message))

    conn.close()

    # -----------------------------
    # Calculate DQ Score
    # -----------------------------
    passed_checks = total_checks - total_failures

    dq_score = round((passed_checks / total_checks) * 100, 2) if total_checks > 0 else 0

    execute("""
    INSERT INTO dq_run_summary
    (run_id, run_time, total_checks, passed_checks, failed_checks, dq_score)
    VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        run_id,
        run_time,
        total_checks,
        passed_checks,
        total_failures,
        dq_score
    ))

    # -----------------------------
    # Summary
    # -----------------------------
    print("\n✅ DQMP Run Complete")
    print(f"🕒 Run ID        : {run_id}")
    print(f"📊 Total Checks  : {total_checks}")
    print(f"✅ Passed        : {passed_checks}")
    print(f"❌ Failures      : {total_failures}")
    print(f"📈 DQ Score      : {dq_score}%")
    print(f"🚨 Alerts        : {len(failures)}")


if __name__ == "__main__":
    main()