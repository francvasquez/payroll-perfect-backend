# db_utils.py
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Create connection string
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    """Create and return a SQLAlchemy engine."""
    return create_engine(DATABASE_URL)


def sanitize_table_name(name):
    """
    Sanitize a string to be safe for use as a table name.
    - Converts to lowercase
    - Replaces spaces and special chars with underscores
    - Removes consecutive underscores
    """
    # Convert to lowercase and replace spaces/special chars with underscores
    sanitized = re.sub(r"[^\w]+", "_", name.lower())
    # Remove consecutive underscores
    sanitized = re.sub(r"_+", "_", sanitized)
    # Remove leading/trailing underscores
    sanitized = sanitized.strip("_")
    return sanitized


def generate_table_name(data_type, client_name):
    """
    Generate a table name following convention: {data_type}_{client_name}]

    Parameters:
    -----------
    data_type : str
        Type of data (e.g., 'ta' for time & attendance)
    client_name : str
        Client/company name

    Returns:
    --------
    str
        Sanitized table name

    Examples:
    ---------
    >>> generate_table_name('ta', 'Acme Corp')
    'ta_acme_corp'
    """
    parts = [sanitize_table_name(data_type), sanitize_table_name(client_name)]

    return "_".join(parts)


def save_to_database(df, data_type="ta", client_name=None, table_name=None):
    """
    Save DataFrame to PostgreSQL with upsert logic.
    ID + Punch In combination is unique and will overwrite existing rows.

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing payroll data with 'ID' and 'Punch In' columns
    data_type : str
        Type of data (default: 'ta' for time & attendance)
    client_name : str
        Client/company name (required if table_name not provided)
    table_name : str, optional
        Explicit table name (overrides generated name)

    Examples:
    ---------
    # All data for a client in one table
    save_to_database(df, data_type='ta', client_name='Acme Corp')

    # Custom table name
    save_to_database(df, table_name='my_custom_table')
    """

    # Validate required columns
    if "ID" not in df.columns or "Punch In" not in df.columns:
        raise ValueError("DataFrame must have 'ID' and 'Punch In' columns")

    # Generate table name if not provided
    if table_name is None:
        if client_name is None:
            raise ValueError("Either table_name or client_name must be provided")
        table_name = generate_table_name(data_type, client_name)
    else:
        table_name = sanitize_table_name(table_name)

    # Add metadata columns
    df["last_updated"] = pd.Timestamp.now()
    if client_name and "client_name" not in df.columns:
        df["client_name"] = client_name

    # Create database engine
    engine = get_engine()

    # Create a temporary table name
    temp_table = f"{table_name}_temp"

    try:
        with engine.connect() as conn:
            # Create the main table if it doesn't exist (first time only)
            df.head(0).to_sql(table_name, conn, if_exists="append", index=False)

            # Create unique constraint on ID + Punch In if not exists
            constraint_name = f"{table_name}_id_Punch In_key"
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
                        UNIQUE ("ID", "Punch In");
                    END IF;
                END $$;
            """
                )
            )
            conn.commit()

            # Write data to temporary table
            df.to_sql(temp_table, conn, if_exists="replace", index=False)

            # Get all column names except ID and Punch In for the UPDATE clause
            update_cols = [col for col in df.columns if col not in ["ID", "Punch In"]]
            update_clause = ", ".join(
                [f'"{col}" = EXCLUDED."{col}"' for col in update_cols]
            )

            # Upsert: Insert new rows, update existing ones
            upsert_query = f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_table}
                ON CONFLICT ("ID", "Punch In") 
                DO UPDATE SET {update_clause};
            """

            conn.execute(text(upsert_query))
            conn.commit()

            # Drop temporary table
            conn.execute(text(f"DROP TABLE {temp_table}"))
            conn.commit()

            # Create indexes for better query performance
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name}("ID")'
                )
            )
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS idx_{table_name}_Punch In ON {table_name}("Punch In")'
                )
            )

            # Create index on pay_period if it exists
            if "pay_period" in df.columns:
                conn.execute(
                    text(
                        f'CREATE INDEX IF NOT EXISTS idx_{table_name}_pay_period ON {table_name}("pay_period")'
                    )
                )

            conn.commit()

        print(f"✓ Successfully saved {len(df)} rows to database")
        print(f"  Table: {table_name}")
        print(f"  Unique constraint: ID + Punch In")

        return table_name

    except Exception as e:
        print(f"✗ Error saving to database: {e}")
        raise


def query_payroll_data(
    table_name=None,
    data_type="ta",
    client_name=None,
    employee_id=None,
    start_date=None,
    end_date=None,
    pay_period=None,
    limit=100,
):
    """
    Query payroll data with optional filters.

    Parameters:
    -----------
    table_name : str, optional
        Explicit table name (overrides generated name)
    data_type : str
        Type of data (default: 'ta')
    client_name : str
        Client/company name (required if table_name not provided)
    employee_id : str or int, optional
        Filter by specific employee ID
    start_date : str or datetime, optional
        Filter records from this date onwards
    end_date : str or datetime, optional
        Filter records up to this date
    pay_period : str, optional
        Filter by specific pay period
    limit : int
        Maximum number of records to return (default: 100)

    Returns:
    --------
    pandas.DataFrame
        Filtered payroll data
    """
    # Generate table name if not provided
    if table_name is None:
        if client_name is None:
            raise ValueError("Either table_name or client_name must be provided")
        table_name = generate_table_name(data_type, client_name)
    else:
        table_name = sanitize_table_name(table_name)

    engine = get_engine()

    query = f"SELECT * FROM {table_name} WHERE 1=1"
    params = {}

    if employee_id is not None:
        query += ' AND "ID" = :employee_id'
        params["employee_id"] = str(employee_id)

    if start_date:
        query += ' AND "Punch In" >= :start_date'
        params["start_date"] = start_date

    if end_date:
        query += ' AND "Punch In" <= :end_date'
        params["end_date"] = end_date

    if pay_period:
        query += ' AND "pay_period" = :pay_period'
        params["pay_period"] = pay_period

    query += f' ORDER BY "Punch In" DESC LIMIT {limit}'

    df = pd.read_sql(query, engine, params=params)
    return df


def list_tables(pattern=None):
    """
    List all tables in the database, optionally filtered by pattern.

    Parameters:
    -----------
    pattern : str, optional
        SQL LIKE pattern (e.g., 'ta_%' for all TA tables)

    Returns:
    --------
    list
        List of table names
    """
    engine = get_engine()

    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """

    params = {}
    if pattern:
        query += " AND table_name LIKE :pattern"
        params["pattern"] = pattern

    query += " ORDER BY table_name"

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        return [row[0] for row in result]


def get_table_info(table_name):
    """
    Get row count and basic info about a table.

    Parameters:
    -----------
    table_name : str
        Name of the table

    Returns:
    --------
    dict
        Dictionary with table statistics
    """
    engine = get_engine()

    with engine.connect() as conn:
        # Get row count
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        row_count = result.fetchone()[0]

        # Get date range
        result = conn.execute(
            text(f'SELECT MIN("Punch In"), MAX("Punch In") FROM {table_name}')
        )
        date_range = result.fetchone()

        return {
            "table_name": table_name,
            "row_count": row_count,
            "earliest_punch": date_range[0],
            "latest_punch": date_range[1],
        }
