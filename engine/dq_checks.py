# ==============================
# DQ CHECKS ENGINE
# ==============================

# ------------------------------
# VOLUME CHECK
# ------------------------------
def volume_check(table_name, expected_min, expected_max, conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}")
    count = cursor.fetchone()["cnt"]
    cursor.close()

    status = "PASS" if expected_min <= count <= expected_max else "FAIL"
    threshold = f"{expected_min}-{expected_max}"

    return count, threshold, status


# ------------------------------
# NULL CHECK
# ------------------------------
def null_check(table_name, column_name, is_nullable, conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"""
        SELECT SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS null_pct
        FROM {table_name}
    """)
    null_pct = cursor.fetchone()["null_pct"]
    cursor.close()

    if is_nullable == 0 and null_pct > 0:
        status = "FAIL"
    else:
        status = "PASS"

    threshold = "NOT_NULL" if is_nullable == 0 else "NULL_ALLOWED"
    return null_pct, threshold, status


# ------------------------------
# FRESHNESS CHECK
# ------------------------------
def freshness_check(table_name, column_name, sla_minutes, conn):
    # Skip freshness if SLA not defined
    if sla_minutes is None:
        return None, None, "SKIP"

    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT TIMESTAMPDIFF(MINUTE, MAX({column_name}), NOW()) AS freshness
        FROM {table_name}
    """)
    freshness = cursor.fetchone()[0]
    cursor.close()

    if freshness is None:
        return None, sla_minutes, "SKIP"

    status = "PASS" if freshness <= sla_minutes else "FAIL"
    return freshness, sla_minutes, status


# ------------------------------
# DUPLICATE CHECK
# ------------------------------
def duplicate_check(table_name, column_name, conn):

    cursor = conn.cursor()

    query = f"""
    SELECT COUNT(*)
    FROM (
        SELECT {column_name}
        FROM {table_name}
        GROUP BY {column_name}
        HAVING COUNT(*) > 1
    ) t
    """

    cursor.execute(query)
    dup_count = cursor.fetchone()[0]
    cursor.close()

    status = "PASS" if dup_count == 0 else "FAIL"
    threshold = "NO_DUPLICATES"

    return dup_count, threshold, status


# ------------------------------
# REFERENTIAL INTEGRITY CHECK
# ------------------------------
def referential_check(
        table_name,
        column_name,
        ref_table,
        ref_column,
        conn):

    cursor = conn.cursor()

    query = f"""
    SELECT COUNT(*)
    FROM {table_name} t
    LEFT JOIN {ref_table} r
    ON t.{column_name} = r.{ref_column}
    WHERE r.{ref_column} IS NULL
    """

    cursor.execute(query)
    orphan_count = cursor.fetchone()[0]
    cursor.close()

    status = "PASS" if orphan_count == 0 else "FAIL"
    threshold = "NO_ORPHANS"

    return orphan_count, threshold, status


## ------------------------------
# SCHEMA DRIFT CHECK
# ------------------------------
def schema_drift_check(table_name, conn):

    cursor = conn.cursor()

    # -----------------------------
    # Expected schema (baseline)
    # -----------------------------
    cursor.execute("""
        SELECT column_name
        FROM table_schema_baseline
        WHERE table_name = %s
    """, (table_name,))
    expected_columns = [row[0] for row in cursor.fetchall()]

    # -----------------------------
    # Actual schema (current DB)
    # -----------------------------
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
        AND table_name = %s
    """, (table_name,))
    actual_columns = [row[0] for row in cursor.fetchall()]

    cursor.close()

    # -----------------------------
    # Compare schemas
    # -----------------------------
    missing_columns = list(set(expected_columns) - set(actual_columns))
    extra_columns = list(set(actual_columns) - set(expected_columns))

    drift_count = len(missing_columns) + len(extra_columns)

    threshold = "NO_SCHEMA_DRIFT"

    if drift_count == 0:
        status = "PASS"
        details = "NO_DRIFT"
    else:
        status = "FAIL"
        details = f"missing:{missing_columns}, extra:{extra_columns}"

    return drift_count, threshold, status, details