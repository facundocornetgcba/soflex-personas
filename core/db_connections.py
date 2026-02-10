"""
Database connection utilities for Neon PostgreSQL.

This module centralizes all database connection logic to avoid duplication
and make credential management easier.
"""

import os
from sqlalchemy import create_engine
import pandas as pd


def get_neon_connection_string():
    """
    Returns the Neon PostgreSQL connection string.
    
    Priority:
    1. Environment variable DATABASE_URL (for GitHub Actions)
    2. Hardcoded connection string (fallback)
    
    Returns:
        str: PostgreSQL connection string
    """
    # Try environment variable first (GitHub Actions)
    conn_str = os.getenv('DATABASE_URL')
    
    if conn_str:
        print("✅ Using DATABASE_URL from environment variable")
        return conn_str
    
    # Fallback to hardcoded connection string
    conn_str = (
        "postgresql://neondb_owner:npg_3X7LoQzdmWhH"
        "@ep-square-paper-a89hpeb2-pooler.eastus2.azure.neon.tech"
        "/neondb?sslmode=require&channel_binding=require"
    )
    print("⚠️ Using hardcoded DATABASE_URL (dev mode)")
    return conn_str


def get_neon_engine():
    """
    Creates and returns a SQLAlchemy engine for Neon PostgreSQL.
    
    Returns:
        sqlalchemy.engine.Engine: Configured database engine
    """
    conn_str = get_neon_connection_string()
    return create_engine(conn_str)


def upload_to_neon_incremental(df, table_name, if_exists='append'):
    """
    Uploads a DataFrame to Neon PostgreSQL.
    
    This is a parametrizable version that allows choosing between
    'replace' (full load) and 'append' (incremental load).
    
    Args:
        df (pd.DataFrame): DataFrame to upload
        table_name (str): Name of the target table
        if_exists (str): What to do if table exists: 'replace', 'append', or 'fail'
                        Default: 'append' for incremental loading
    
    Raises:
        Exception: If upload fails
    """
    print(f"⬆️ Iniciando carga a Neon PostgreSQL: tabla '{table_name}' (mode: {if_exists})...")
    
    try:
        engine = get_neon_engine()
        
        # Upload dataframe to PostgreSQL
        df.to_sql(
            table_name, 
            engine, 
            if_exists=if_exists, 
            index=False,
            method='multi',  # Optimize for bulk inserts
            chunksize=1000   # Insert in chunks to avoid memory issues
        )
        
        print(f"✅ Carga a Neon PostgreSQL exitosa: {len(df)} registros")
        
    except Exception as e:
        print(f"❌ Error subiendo a Neon: {e}")
        # Re-raise para que el proceso falle visiblemente si no se puede guardar
        raise e
    finally:
        engine.dispose()


def get_table_stats(table_name):
    """
    Gets basic statistics about a table in Neon.
    
    Useful for validation and debugging.
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        dict: Statistics (count, min_date, max_date)
    """
    try:
        engine = get_neon_engine()
        
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            MIN("Fecha Inicio") as min_fecha,
            MAX("Fecha Inicio") as max_fecha
        FROM {table_name}
        """
        
        result = pd.read_sql(query, engine)
        
        return {
            'total_records': result['total_records'][0],
            'min_fecha': result['min_fecha'][0],
            'max_fecha': result['max_fecha'][0]
        }
        
    except Exception as e:
        print(f"⚠️ Error obteniendo estadísticas de {table_name}: {e}")
        return None
    finally:
        engine.dispose()
