# db_utils.py
import pandas as pd
from sqlalchemy import create_engine, text
import os, uuid

# Database configuration
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Create connection string
DATABASE_URL = (
    f"postgresql+pg8000://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


# def get_engine():
#     """Create and return a SQLAlchemy engine."""
#     return create_engine(DATABASE_URL)


def get_engine():
    """Create and return a SQLAlchemy engine with connection pooling disabled."""
    # pg8000 works better in Lambda with pooling disabled
    return create_engine(
        DATABASE_URL,
        poolclass=None,  # Disable connection pooling for Lambda
        connect_args={"timeout": 30},  # 30 second timeout
    )


def save_to_database(df, table_name, client_name):
    """
    Save DataFrame to PostgreSQL with upsert logic.
    Uses proper column types inferred from pandas DataFrame.
    Only indexes the unique constraint (ID + In Punch).

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with 'ID' and 'In Punch' columns
    table_name : str
        Base table name (e.g., 'ta', 'payroll')
    client_name : str
        Client identifier (e.g., 'demo_client', 'acme_corp')

    Returns:
    --------
    str : Full table name created (e.g., 'demo_client_ta')
    """

    if "ID" not in df.columns or "In Punch" not in df.columns:
        raise ValueError("DataFrame must have 'ID' and 'In Punch' columns")

    # Generate full table name
    table_name = f"{client_name}_{table_name}"

    # Prepare DataFrame
    df = df.copy()
    df["last_updated"] = pd.Timestamp.now()

    if "client_name" not in df.columns:
        df["client_name"] = client_name

    engine = get_engine()
    temp_table = f"{table_name}_temp_{uuid.uuid4().hex[:8]}"

    try:
        with engine.begin() as conn:
            # Create table if it doesn't exist
            # pandas will infer proper column types (INTEGER, DOUBLE PRECISION, TEXT, TIMESTAMP, etc.)
            df.head(0).to_sql(table_name, conn, if_exists="append", index=False)

            # Get existing columns
            result = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = :t"
                ),
                {"t": table_name},
            )
            existing_columns = {row[0] for row in result}

            # Add new columns if they don't exist
            # Let pandas handle type inference by using a temp table
            new_cols = [col for col in df.columns if col not in existing_columns]
            if new_cols:
                # Create a small temp table to infer types
                type_inference_temp = f"{table_name}_types_{uuid.uuid4().hex[:8]}"
                df.head(1).to_sql(
                    type_inference_temp, conn, if_exists="replace", index=False
                )

                # Get the types that pandas chose
                type_result = conn.execute(
                    text(
                        """
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = :t
                    """
                    ),
                    {"t": type_inference_temp},
                )
                inferred_types = {row[0]: row[1] for row in type_result}

                # Add new columns with inferred types
                for col in new_cols:
                    pg_type = inferred_types.get(col, "TEXT")
                    print(f"  Adding new column: {col} ({pg_type})")
                    conn.execute(
                        text(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {pg_type}')
                    )

                # Drop the type inference temp table
                conn.execute(text(f"DROP TABLE {type_inference_temp}"))

            # Create unique constraint if not exists (only essential index)
            constraint_name = f"{table_name}_id_punchin_key"
            conn.execute(
                text(
                    f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint 
                            WHERE conname = '{constraint_name}'
                        ) THEN
                            ALTER TABLE {table_name} 
                            ADD CONSTRAINT {constraint_name} 
                            UNIQUE ("ID", "In Punch");
                        END IF;
                    END $$;
                """
                )
            )

            # Write to temporary table
            df.to_sql(temp_table, conn, if_exists="replace", index=False)

            # Upsert: insert new rows, update existing
            update_cols = [col for col in df.columns if col not in ["ID", "In Punch"]]
            update_clause = ", ".join(
                [f'"{col}" = EXCLUDED."{col}"' for col in update_cols]
            )

            upsert_query = f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_table}
                ON CONFLICT ("ID", "In Punch")
                DO UPDATE SET {update_clause};
            """
            conn.execute(text(upsert_query))

            # Drop temporary table
            conn.execute(text(f"DROP TABLE {temp_table}"))

        print(f"✓ Successfully saved {len(df)} rows to table: {table_name}")
        return table_name

    except Exception as e:
        print(f"✗ Error saving to database: {e}")
        raise


# def query_payroll_data(
#     table_name=None,
#     data_type="ta",
#     client_name=None,
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
#     client_name : str
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
#         if client_name is None:
#             raise ValueError("Either table_name or client_name must be provided")
#         table_name = generate_table_name(data_type, client_name)
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
