# db_utils.py
import pandas as pd
import numpy as np
import os, uuid
import psycopg2
from psycopg2.extras import execute_values
import config
import client_config
import logging
import json

# Consider extendind accross other files
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Database configuration from Env Vars
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


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
            col for sublist in config.COLUMN_TO_KEEP_DB.values() for col in sublist
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
            c for c in all_cols if c not in client_config.EXCLUDE_FROM_PULLDOWN
        ]

        cur.close()
        conn.close()

        return {
            "statusCode": 200,
            "headers": config.CORS_HEADERS,
            "body": json.dumps(selectable_cols),
        }
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def handle_query_ta_records(clientId, employeeId, startDate, endDate, selectedCols):
    try:
        conn = get_db_connection()  # Use your existing connection engine
        # --- ADD THIS CHECK ---
        if conn is None:
            return {
                "statusCode": 503,  # Service Unavailable
                "headers": config.CORS_HEADERS,
                "body": json.dumps(
                    {"error": "Database is currently unreachable. Check network/VPN."}
                ),
            }
        # -----------------------
        cur = conn.cursor()

        table_name = f"{clientId}_ta"

        # 1. Always display these on the query:
        base_cols = ["Employee", "In Punch", "Out Punch"]

        # 2. Add the user-selected extra columns
        # We wrap in double quotes to handle spaces/special chars safely
        all_cols_to_query = base_cols + selectedCols
        quoted_cols = [f'"{col}"' for col in all_cols_to_query]
        select_clause = ", ".join(quoted_cols)

        # 3. Build the WHERE clause dynamically
        query = f"SELECT {select_clause} FROM {table_name} WHERE 1=1"
        params = []

        if employeeId:
            query += ' AND "ID" = %s'
            params.append(employeeId)

        if startDate and endDate:
            query += ' AND "Pay Date" BETWEEN %s AND %s'
            params.append(startDate)
            params.append(endDate)

        query += ' ORDER BY "In Punch" DESC LIMIT 1000'  # Safety cap

        cur.execute(query, tuple(params))

        # 4. Map results to a list of dictionaries for the React UI
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in cur.fetchall()]

        # Close connection
        cur.close()
        conn.close()

        return {
            "statusCode": 200,
            "headers": config.CORS_HEADERS,
            "body": json.dumps(
                results, default=str
            ),  # default=str handles Date/Timestamp conversion
        }

    except Exception as e:
        print(f"Query Error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": config.CORS_HEADERS,
            "body": json.dumps({"error": f"Database query failed: {str(e)}"}),
        }
