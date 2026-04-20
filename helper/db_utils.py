# db_utils.py
import pandas as pd
import numpy as np
import os, uuid
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import app_config
import logging
import json
from datetime import datetime, timedelta

# Consider extendind accross other files
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Database configuration from Env Vars
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def worker_save_ta(df, clientId, pay_date):
    """Worker thread for raw punches. Gets its own isolated connection."""
    conn = get_db_connection()
    if not conn:
        raise ConnectionError("Raw TA Worker: DB connection failed.")
    try:
        save_ta_to_db(df, clientId, pay_date, conn)
    finally:
        conn.close()


def worker_save_daily(daily_df, clientId, pay_date):
    """Worker thread for daily totals. Gets its own isolated connection."""
    conn = get_db_connection()
    if not conn:
        raise ConnectionError("Daily DF Worker: DB connection failed.")
    try:
        save_daily_df_to_db(daily_df, clientId, pay_date, conn)
    finally:
        conn.close()


def save_daily_df_to_db(
    daily_df: pd.DataFrame, clientId: str, target_pay_date: str, conn
):
    """
    Saves the daily_df to PostgreSQL using a transactional Wipe & Reload pattern
    to prevent ghost records, and dynamically manages schema evolution.
    """
    if daily_df.empty:
        logger.info("daily_df is empty. Nothing to save.")
        return

    # 1. Prepare Metadata and Table Naming
    # PostgreSQL prefers lowercase table names to avoid quoting headaches
    table_name = f"{clientId}_daily_df".lower()

    df = daily_df.copy()
    df["Last_Updated"] = pd.Timestamp.now(tz="America/Los_Angeles")

    # Critical: psycopg2 crashes on Pandas NaN/NaT. Convert them to Python None (SQL NULL)
    df = df.where(pd.notnull(df), None)

    cursor = conn.cursor()

    try:
        # --- 2. DYNAMIC SCHEMA MANAGEMENT ---
        # Map Pandas data types to PostgreSQL data types
        def get_pg_type(dtype):
            dtype_str = str(dtype)
            if "int" in dtype_str:
                return "INTEGER"
            if "float" in dtype_str:
                return "NUMERIC"
            if "bool" in dtype_str:
                return "BOOLEAN"
            if "datetime" in dtype_str:
                return "TIMESTAMP WITH TIME ZONE"
            return "TEXT"

        # Check if table exists
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            );
        """,
            (table_name,),
        )

        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Auto-create the table with the composite primary key
            cols = [f'"{col}" {get_pg_type(dtype)}' for col, dtype in df.dtypes.items()]
            create_query = f"""
                CREATE TABLE {table_name} (
                    {', '.join(cols)}, 
                    PRIMARY KEY ("ID", "Attributed_Workday")
                );
            """
            cursor.execute(create_query)
        else:
            # Check for missing columns and auto-alter the table
            cursor.execute(
                """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s;
            """,
                (table_name,),
            )
            existing_cols_lower = [row[0].lower() for row in cursor.fetchall()]

            for col in df.columns:
                # Postgres lowercases column names in information_schema unless they were created with quotes
                if col.lower() not in existing_cols_lower:
                    alter_query = f'ALTER TABLE {table_name} ADD COLUMN "{col}" {get_pg_type(df[col].dtype)};'
                    cursor.execute(alter_query)

        # --- 2.5 CREATE FAST LOOKUP INDEX ON Fiscal_Pay_Date ---
        index_name = f"idx_{table_name}_pay_date"
        index_query = f"""
            CREATE INDEX IF NOT EXISTS {index_name} 
            ON {table_name} ("Fiscal_Pay_Date");
        """
        cursor.execute(index_query)

        # --- 3. WIPE AND RELOAD (TRANSACTIONAL) ---
        # Erase existing records for this specific pay period to clear stragglers
        delete_query = f'DELETE FROM {table_name} WHERE "Fiscal_Pay_Date" = %s;'
        # Note: If target_pay_date is a string, we pass it directly.
        cursor.execute(delete_query, (str(target_pay_date),))

        # --- 4. BULK INSERT ---
        columns = [f'"{col}"' for col in df.columns]
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

        # Convert dataframe to a list of tuples for psycopg2
        values = [tuple(row) for row in df.to_numpy()]

        # execute_values is highly optimized for bulk inserts in psycopg2
        execute_values(cursor, insert_query, values)

        # Commit the entire Wipe + Insert transaction
        conn.commit()
        logger.info(
            f"Successfully saved {len(df)} rows to {table_name} for pay date {target_pay_date}."
        )

    except Exception as e:
        # If anything fails (like a bad data type), undo the DELETE so we don't lose data
        conn.rollback()
        logger.error(f"Error saving daily_df to DB: {e}")
        raise e
    finally:
        cursor.close()


def save_ta_to_db(df, clientId, pay_date, conn):

    # Add Metadata
    df["Last Updated"] = pd.Timestamp.now(tz="America/Los_Angeles")
    df["Pay Date"] = pay_date

    # Identify which rows have duplicate keys - if there are, the write will crash
    # as the logic will not know what to do.
    duplicate_mask = df.duplicated(subset=["ID", "In Punch"], keep=False)
    duplicates = df[duplicate_mask].sort_values(by=["ID", "In Punch"])

    if not duplicates.empty:
        print(f"⚠️ Found {len(duplicates)} rows with duplicate 'ID' + 'In Punch' keys!")
        # Print the first few duplicates to the logs for inspection
        print(duplicates[["ID", "In Punch", "Employee", "Location"]].head(10))

    # Filter DF to COLUMN_TO_KEEP_DB
    try:
        cols_to_keep = [
            col for sublist in app_config.COLUMN_TO_KEEP_DB.values() for col in sublist
        ]
        df = df[cols_to_keep].copy()
    except KeyError as e:
        print(f"Column missing from DataFrame: {e}")
        raise

    # Create tables if it doesn't exist
    full_table_name = f"{clientId}_ta"
    temp_table = f"temp_upsert_{uuid.uuid4().hex[:8]}"
    print(f"Connected to DB - preparing to upsert to {full_table_name}")

    try:
        # 'with conn' handles the COMMIT at the end automatically
        with conn:
            # ALL database steps must be inside this 'with cur' block
            with conn.cursor() as cur:

                # 1. Create table
                cols_sql = ", ".join(
                    [f'"{c}" {get_pg_type(df[c].dtype)}' for c in df.columns]
                )
                cur.execute(
                    f'CREATE TABLE IF NOT EXISTS "{full_table_name}" ({cols_sql});'
                )

                # 2. Schema evolution - add new cols if they don't exist
                cur.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    (full_table_name,),
                )
                existing_db_cols = {row[0] for row in cur.fetchall()}
                new_cols = [c for c in df.columns if c not in existing_db_cols]

                for col in new_cols:
                    pg_type = get_pg_type(df[col].dtype)
                    print(f"Adding new column: {col}")
                    cur.execute(
                        f'ALTER TABLE "{full_table_name}" ADD COLUMN "{col}" {pg_type};'
                    )

                # 3. Constraint - ensure unique on ID + In Punch
                constraint_name = f"uq_{full_table_name}"
                cur.execute(
                    f"""
                    DO $$ BEGIN
                        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{constraint_name}') THEN
                            ALTER TABLE "{full_table_name}" ADD CONSTRAINT "{constraint_name}" UNIQUE ("ID", "In Punch");
                        END IF;
                    END $$;
                """
                )

                # 3b. Add Index on "Pay Date" for fast deletions if not created already
                # We use the clientId in the name to keep it unique across the DB
                index_name = f"idx_{clientId}_pd"
                print(f"Ensuring index {index_name} exists on {full_table_name}")
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{full_table_name}" ("Pay Date");'
                )

                # 4. Temp table for upsert
                cur.execute(
                    f'CREATE TEMP TABLE "{temp_table}" ({cols_sql}) ON COMMIT DROP;'
                )

                # 5. Bulk insert
                data = [tuple(x) for x in df.replace({np.nan: None}).to_numpy()]
                cols_str = ", ".join([f'"{c}"' for c in df.columns])
                insert_query = f'INSERT INTO "{temp_table}" ({cols_str}) VALUES %s'
                execute_values(cur, insert_query, data, page_size=2000)

                # 6. Upsert
                update_cols = [c for c in df.columns if c not in ["ID", "In Punch"]]
                update_clause = ", ".join(
                    [f'"{c}" = EXCLUDED."{c}"' for c in update_cols]
                )
                upsert_query = f"""
                    INSERT INTO "{full_table_name}" ({cols_str})
                    SELECT * FROM "{temp_table}"
                    ON CONFLICT ("ID", "In Punch")
                    DO UPDATE SET {update_clause};
                """
                cur.execute(upsert_query)

        print(f"✓ Successfully upserted {len(df)} rows to {full_table_name}")

    except Exception as e:
        # Re-raise the error so lambda_handler knows it failed
        print(f"Error during DB transaction: {e}")
        raise e
    finally:
        # Do NOT put a return statement here
        print("Closing database cursor logic.")


def get_db_connection():
    """
    Attempts to connect to the DB, graceful exit continues code if unreachable.
    Returns the connection object if successful, or None if the DB is unreachable.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            connect_timeout=5,  # Reduced timeout for faster feedback
        )
        return conn
    except psycopg2.OperationalError as e:
        # This catches "Connection Refused" (Instance paused)
        logger.warning(
            f"NOTICE: Database is currently unreachable or paused. Skipping DB operations. Details: {e}"
        )
        return None
    except Exception as e:
        # Catches other issues like wrong passwords
        logger.error(f"ERROR: Unexpected connection error: {e}")
        return None


def get_pg_type(dtype):
    """Maps pandas dtypes to PostgreSQL types."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    return "TEXT"


def delete_ta_from_db(conn, clientId, pay_date):
    """
    Deletes all rows for a specific pay date from ta table.
    """
    full_table_name = f"{clientId}_ta"

    try:
        # 'with conn' handles the COMMIT/ROLLBACK
        with conn:
            with conn.cursor() as cur:
                # 1. Verify table exists to prevent a 42P01 (Undefined Table) error
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    );
                """,
                    (full_table_name,),
                )

                if not cur.fetchone()[0]:
                    print(
                        f"Table {full_table_name} does not exist. Skipping DB delete."
                    )
                    return 0

                # 2. Execute the deletion
                # Table name is string-formatted (safe if internal), value is parameterized
                query = f'DELETE FROM "{full_table_name}" WHERE "Pay Date" = %s'
                cur.execute(query, (pay_date,))

                deleted_rows = cur.rowcount
                print(
                    f"✓ Successfully deleted {deleted_rows} rows from {full_table_name} for {pay_date}"
                )
                return deleted_rows

    except Exception as e:
        print(f"Error deleting from database table {full_table_name}: {e}")
        raise e


def handle_get_ta_columns(clientId):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        table_name = f"{clientId}_ta"

        # Query PostgreSQL metadata for column names
        cur.execute(
            f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        )

        all_cols = [row[0] for row in cur.fetchall()]

        # Columns to EXCLUDE from the pulldown (User shouldn't pick these)
        selectable_cols = [
            c for c in all_cols if c not in app_config.EXCLUDE_FROM_PULLDOWN
        ]

        cur.close()
        conn.close()

        return {
            "statusCode": 200,
            "headers": app_config.CORS_HEADERS,
            "body": json.dumps(selectable_cols),
        }
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def handle_query_ta_records(clientId, employeeId, startDate, endDate, selectedCols):
    try:
        conn = get_db_connection()
        if conn is None:
            return {
                "statusCode": 503,
                "headers": app_config.CORS_HEADERS,
                "body": json.dumps(
                    {"error": "Database is currently unreachable. Check network/VPN."}
                ),
            }

        cur = conn.cursor()

        # ==========================================
        # 1. FETCH RAW PUNCHES (The "Cause")
        # ==========================================
        raw_table_name = f"{clientId}_ta"
        base_cols = ["ID", "In Punch", "Out Punch", "Employee"]
        all_cols = base_cols + [c for c in selectedCols if c not in base_cols]

        select_clause = sql.SQL(", ").join(map(sql.Identifier, all_cols))
        raw_query = sql.SQL("SELECT {fields} FROM {table} WHERE 1=1").format(
            fields=select_clause, table=sql.Identifier(raw_table_name)
        )

        raw_params = []
        exclusive_end = None

        if employeeId:
            raw_query += sql.SQL(' AND "ID" = %s')
            raw_params.append(employeeId)

        if startDate and endDate:
            end_dt = datetime.strptime(endDate, "%Y-%m-%d") + timedelta(days=1)
            exclusive_end = end_dt.strftime("%Y-%m-%d")

            raw_query += sql.SQL(' AND "In Punch" >= %s AND "In Punch" < %s')
            raw_params.append(startDate)
            raw_params.append(exclusive_end)

        raw_query += sql.SQL(' ORDER BY "In Punch" ASC LIMIT 300')
        cur.execute(raw_query, tuple(raw_params))

        raw_columns = [desc[0] for desc in cur.description]
        raw_results = [dict(zip(raw_columns, row)) for row in cur.fetchall()]

        # ==========================================
        # 2. FETCH DAILY TOTALS (The "Effect")
        # ==========================================
        daily_results = []
        daily_table_name = f"{clientId}_daily_df"

        # Safety Check: Does the daily_df table exist for this client yet?
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            );
        """,
            (daily_table_name,),
        )

        if cur.fetchone()[0]:
            # Table exists, let's grab the aggregated data
            # Selecting all columns (*) so React has access to everything
            daily_query = sql.SQL("SELECT * FROM {table} WHERE 1=1").format(
                table=sql.Identifier(daily_table_name)
            )
            daily_params = []

            if employeeId:
                daily_query += sql.SQL(' AND "ID" = %s')
                daily_params.append(employeeId)

            if startDate and endDate and exclusive_end:
                # We can safely reuse the inclusive start and exclusive end
                daily_query += sql.SQL(
                    ' AND "Attributed_Workday" >= %s AND "Attributed_Workday" < %s'
                )
                daily_params.append(startDate)
                daily_params.append(exclusive_end)

            daily_query += sql.SQL(' ORDER BY "Attributed_Workday" ASC')
            cur.execute(daily_query, tuple(daily_params))

            daily_columns = [desc[0] for desc in cur.description]
            daily_results = [dict(zip(daily_columns, row)) for row in cur.fetchall()]

        cur.close()
        conn.close()

        # ==========================================
        # 3. PACKAGE AND RETURN SPLIT DATA
        # ==========================================
        split_data = {"daily_totals": daily_results, "raw_punches": raw_results}

        return {
            "statusCode": 200,
            "headers": app_config.CORS_HEADERS,
            "body": json.dumps(
                split_data, default=str  # Handles Date/Timestamp conversion
            ),
        }

    except Exception as e:
        print(f"Query Error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": app_config.CORS_HEADERS,
            "body": json.dumps({"error": f"Database query failed: {str(e)}"}),
        }
