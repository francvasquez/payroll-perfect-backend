# db_utils.py
import pandas as pd
import numpy as np
import os, uuid
import psycopg2
from psycopg2.extras import execute_values

# Database configuration from Env Vars
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


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


def save_to_database_fast(df, table_name, clientId):
    if not clientId or clientId == "None":
        raise ValueError("clientId is missing from input!")

    full_table_name = f"{clientId}_{table_name}"
    # This temp table name stays inside the DB session
    temp_table = f"temp_upsert_{uuid.uuid4().hex[:8]}"
    # CONSIDER MOVING CONNECTION OUTSIDE FOR OTHER CALLS
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=10,
    )
    print(f"Connected to DB - preparing to upsert to {full_table_name}")
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. Create table if it doesn't exist (using DF types)
                cols_sql = ", ".join(
                    [f'"{c}" {get_pg_type(df[c].dtype)}' for c in df.columns]
                )
                cur.execute(
                    f'CREATE TABLE IF NOT EXISTS "{full_table_name}" ({cols_sql});'
                )

                # 2. Ensure the Unique Constraint exists for the UPSERT
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

                # 3. Create a Temp Table (fastest way: copy schema)
                cur.execute(
                    f'CREATE TEMP TABLE "{temp_table}" (LIKE "{full_table_name}" INCLUDING ALL) ON COMMIT DROP;'
                )

                # 4. Bulk Insert to Temp Table
                # Replace NaNs with None so they become NULLs in Postgres
                data = [tuple(x) for x in df.replace({np.nan: None}).to_numpy()]
                cols_str = ", ".join([f'"{c}"' for c in df.columns])
                insert_query = f'INSERT INTO "{temp_table}" ({cols_str}) VALUES %s'
                execute_values(cur, insert_query, data, page_size=2000)

                # 5. Atomic Upsert
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

    finally:
        conn.close()


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
