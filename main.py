from datetime import datetime
from engine import * # Importing all functions from __init__.py for easy access

def main():

    print("\n🚀 Starting Data Quality Monitoring Platform (DQMP) Run")
    print("* Generated Results in dq_results") 
    print("* Generated Alerts  in dq_alerts")
    print("* Generated Summary in dq_run_summary\n")

    run_id = datetime.now()
    run_time = datetime.now()

    conn = get_connection()

    metadata = fetch_all("SELECT * FROM table_metadata")

    tables = {}
    for row in metadata:
        tables.setdefault(row["table_name"], []).append(row)

    total_possible_checks = 0
    executed_checks = 0
    failures = 0

    for table_name, cols in tables.items():

        meta = next(col for col in cols if col["expected_volume_min"] is not None)

        # ---------------- VOLUME ----------------
        vol, threshold, status = volume_check(
            table_name,
            meta["expected_volume_min"],
            meta["expected_volume_max"],
            conn
        )

        insert_dq_result(run_id, run_time, table_name, None, "volume", vol, threshold, status)

        total_possible_checks += 1
        if status != "SKIP":
            executed_checks += 1
            if status == "FAIL":
                failures += 1

        # ---------------- SCHEMA DRIFT ----------------
        drift, threshold, status, details = schema_drift_check(table_name, conn)

        insert_dq_result(
            run_id,
            run_time,
            table_name,
            None,
            "schema_drift",
            drift,
            threshold,
            status,
            details
        )

        total_possible_checks += 1
        if status != "SKIP":
            executed_checks += 1
            if status == "FAIL":
                failures += 1

        # ---------------- COLUMN CHECKS ----------------
        for col in cols:

            column_name = col["column_name"]

            # NULL CHECK
            if column_name:

                null_pct, threshold, status = null_check(
                    table_name,
                    column_name,
                    col["is_nullable"],
                    conn
                )

                insert_dq_result(
                    run_id,
                    run_time,
                    table_name,
                    column_name,
                    "null_pct",
                    null_pct,
                    threshold,
                    status
                )

                total_possible_checks += 1
                if status != "SKIP":
                    executed_checks += 1
                    if status == "FAIL":
                        failures += 1

            # DUPLICATE CHECK
            if col.get("is_primary_key") == 1:

                dup_count, threshold, status = duplicate_check(
                    table_name,
                    column_name,
                    conn
                )

                insert_dq_result(
                    run_id,
                    run_time,
                    table_name,
                    column_name,
                    "duplicate_count",
                    dup_count,
                    threshold,
                    status
                )

                total_possible_checks += 1
                if status != "SKIP":
                    executed_checks += 1
                    if status == "FAIL":
                        failures += 1

            # REFERENTIAL CHECK
            if col.get("reference_table"):

                orphan_count, threshold, status = referential_check(
                    table_name,
                    column_name,
                    col["reference_table"],
                    col["reference_column"],
                    conn
                )

                insert_dq_result(
                    run_id,
                    run_time,
                    table_name,
                    column_name,
                    "orphan_records",
                    orphan_count,
                    threshold,
                    status
                )

                total_possible_checks += 1
                if status != "SKIP":
                    executed_checks += 1
                    if status == "FAIL":
                        failures += 1

            # FRESHNESS CHECK
            if col["freshness_sla_minutes"] is not None:

                freshness, threshold, status = freshness_check(
                    table_name,
                    column_name,
                    col["freshness_sla_minutes"],
                    conn
                )

            else:

                freshness, threshold, status = None, None, "SKIP"

            insert_dq_result(
                run_id,
                run_time,
                table_name,
                column_name,
                "freshness_minutes",
                freshness,
                threshold,
                status
            )

            total_possible_checks += 1
            if status != "SKIP":
                executed_checks += 1
                if status == "FAIL":
                    failures += 1


    
    # ---------------- GENERATE ALERTS ----------------
    failure_rows = fetch_all("""
        SELECT * 
        FROM dq_results
        WHERE run_id=(SELECT MAX(run_id) FROM dq_results)  AND status='FAIL'
    """)

    for row in failure_rows:
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

    passed_checks = executed_checks - failures

    dq_score = round((passed_checks / executed_checks) * 100, 2) if executed_checks > 0 else 0
    coverage = round((executed_checks / total_possible_checks) * 100, 2)

    execute("""
        INSERT INTO dq_run_summary
        (run_id, run_time, total_checks, passed_checks, failed_checks, dq_score)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        run_id,
        run_time,
        executed_checks,
        passed_checks,
        failures,
        dq_score
    ))

    table_list = ", ".join(tables.keys())

    print(f"Run ID        : {run_id}")
    print(f"Tables Scanned: {len(tables)} {{{table_list}}}")

    print("\n────────────────────────────────────")
    print("DATA QUALITY SUMMARY")
    print("────────────────────────────────────")

    print(f"Rules Defined        : {total_possible_checks}")
    print(f"Rules Evaluated      : {executed_checks}")
    print(f"Rules Skipped        : {total_possible_checks - executed_checks}")

    print(f"\nPassed Checks        : {passed_checks}")
    print(f"Failed Checks        : {failures}")

    print("\n────────────────────────────────────")
    print("QUALITY METRICS")
    print("────────────────────────────────────")

    print(f"DQ Score             : {dq_score}%")
    print(f"Coverage             : {coverage}%")

    print("\n────────────────────────────────────")
    print("Metric Definitions")
    print("────────────────────────────────────")

    print("DQ Score  → % of executed checks that passed (Passed Checks / Evaluated Checks)")
    print("Coverage  → % of defined rules that were executed (Evaluated Checks / Total Rules)")


if __name__ == "__main__":
    main()
