# db_utils.py
import pandas as pd
import numpy as np
import os, uuid
import psycopg2
from psycopg2.extras import execute_values
import config
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


def save_to_database_fast(df, table_name, clientId, pay_date, conn):

    # Cleanup before saving
    df = df.drop(columns=config.COLUMNS_TO_DROP_FOR_DATABASE)

    # Metadata
    df = df.copy()
    df["last_updated"] = pd.Timestamp.now()
    df["pay_date"] = pay_date

    # Create tables
    full_table_name = f"{clientId}_{table_name}"
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


def handle_query_ta_records(clientId, employeeId, startDate, endDate, selectedCols):
    try:
        conn = get_db_connection()  # Use your existing connection engine
        cur = conn.cursor()

        table_name = f"{clientId}_ta"

        # 1. Start with the core required columns
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
            query += ' AND "pay_date" BETWEEN %s AND %s'
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


# def query_payroll_data(
#     table_name=None,
#     data_type="ta",
#     clientId=None,
#     employee_id=None,
#     start_date=None,
#     end_date=None,
#     pay_period=None,
#     limit=100,
# ):
#     """
#     Query payroll data with optional filters.

#     Parameters:
#     -----------
#     table_name : str, optional
#         Explicit table name (overrides generated name)
#     data_type : str
#         Type of data (default: 'ta')
#     clientId : str
#         Client/company name (required if table_name not provided)
#     employee_id : str or int, optional
#         Filter by specific employee ID
#     start_date : str or datetime, optional
#         Filter records from this date onwards
#     end_date : str or datetime, optional
#         Filter records up to this date
#     pay_period : str, optional
#         Filter by specific pay period
#     limit : int
#         Maximum number of records to return (default: 100)

#     Returns:
#     --------
#     pandas.DataFrame
#         Filtered payroll data
#     """
#     # Generate table name if not provided
#     if table_name is None:
#         if clientId is None:
#             raise ValueError("Either table_name or clientId must be provided")
#         table_name = generate_table_name(data_type, clientId)
#     else:
#         table_name = sanitize_table_name(table_name)

#     engine = get_engine()

#     query = f"SELECT * FROM {table_name} WHERE 1=1"
#     params = {}

#     if employee_id is not None:
#         query += ' AND "ID" = :employee_id'
#         params["employee_id"] = str(employee_id)

#     if start_date:
#         query += ' AND "In Punch" >= :start_date'
#         params["start_date"] = start_date

#     if end_date:
#         query += ' AND "In Punch" <= :end_date'
#         params["end_date"] = end_date

#     if pay_period:
#         query += ' AND "pay_period" = :pay_period'
#         params["pay_period"] = pay_period

#     query += f' ORDER BY "In Punch" DESC LIMIT {limit}'

#     df = pd.read_sql(query, engine, params=params)
#     return df


# def list_tables(pattern=None):
#     """
#     List all tables in the database, optionally filtered by pattern.

#     Parameters:
#     -----------
#     pattern : str, optional
#         SQL LIKE pattern (e.g., 'ta_%' for all TA tables)

#     Returns:
#     --------
#     list
#         List of table names
#     """
#     engine = get_engine()

#     query = """
#         SELECT table_name
#         FROM information_schema.tables
#         WHERE table_schema = 'public'
#     """

#     params = {}
#     if pattern:
#         query += " AND table_name LIKE :pattern"
#         params["pattern"] = pattern

#     query += " ORDER BY table_name"

#     with engine.connect() as conn:
#         result = conn.execute(text(query), params)
#         return [row[0] for row in result]


# def get_table_info(table_name):
#     """
#     Get row count and basic info about a table.

#     Parameters:
#     -----------
#     table_name : str
#         Name of the table

#     Returns:
#     --------
#     dict
#         Dictionary with table statistics
#     """
#     engine = get_engine()

#     with engine.connect() as conn:
#         # Get row count
#         result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
#         row_count = result.fetchone()[0]

#         # Get date range
#         result = conn.execute(
#             text(f'SELECT MIN("In Punch"), MAX("In Punch") FROM {table_name}')
#         )
#         date_range = result.fetchone()

#         return {
#             "table_name": table_name,
#             "row_count": row_count,
#             "earliest_punch": date_range[0],
#             "latest_punch": date_range[1],
#         }
