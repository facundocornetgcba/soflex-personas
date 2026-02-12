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


def to_snake_case(col_name):
    """Convierte 'Fecha Inicio' a 'fecha_inicio'"""
    return col_name.strip().lower().replace(' ', '_')

def to_title_case(col_name):
    """
    Intenta convertir 'fecha_inicio' a 'Fecha Inicio'.
    NOTA: Esta es una heurística. Para exactitud, se requeriría un mapeo explícito
    si los nombres originales tienen mayúsculas/minúsculas mixtas específicas.
    
    Por ahora, usaremos un mapeo inverso basado en las columnas conocidas.
    """
    # Mapeo explícito para columnas críticas
    MAPPING = {
        'fecha_inicio': 'Fecha Inicio',
        'fecha_fin': 'Fecha Fin',
        'id_suceso': 'Id Suceso',
        'persona_dni': 'Persona DNI',
        'latitud': 'Latitud',
        'longitud': 'Longitud',
        'agencia': 'Agencia',
        'tipo_carta': 'Tipo Carta',
        'origen': 'Origen',
        'tipo': 'Tipo',
        'subtipo': 'SubTipo', # Nota: SubTipo vs Subtipo
        'comuna_calculada': 'comuna_calculada', # Esta suele ser snake_case en el script
        'dni_categorizado': 'DNI_Categorizado'
    }
    return MAPPING.get(col_name, col_name.title().replace('_', ' '))

def download_from_neon(table_name='historico_limpio'):
    """
    Downloads the complete historical dataset from Neon PostgreSQL.
    Maps columns from snake_case (Neon) to Title Case (Drive legacy).
    """
    print(f"⬇️ Descargando histórico desde Neon PostgreSQL: tabla '{table_name}'...")
    
    try:
        engine = get_neon_engine()
        query = f'SELECT * FROM "{table_name}"'
        df = pd.read_sql(query, engine)
        
        # Renombrar columnas de snake_case a Title Case (Logic Legacy)
        # Esto es CRÍTICO para que el resto del código funcione
        
        # 1. Detectar si vienen en snake_case (si 'fecha_inicio' está en columnas)
        if 'fecha_inicio' in df.columns:
            print("🔄 Detectado snake_case en Neon. Renombrando columnas...")
            
            # Crear mapeo dinámico
            new_columns = {}
            for col in df.columns:
                # Mapeo específico crítico + fallback a Title Case
                if col == 'fecha_inicio': new_columns[col] = 'Fecha Inicio'
                elif col == 'fecha_fin': new_columns[col] = 'Fecha Fin'
                elif col == 'id_suceso': new_columns[col] = 'Id Suceso'
                elif col == 'persona_dni': new_columns[col] = 'Persona DNI'
                elif col == 'recurso_fecha_liberado': new_columns[col] = 'Recurso Fecha Liberado'
                elif col == 'recurso_fecha_asignacion': new_columns[col] = 'Recurso Fecha asignacion'
                elif col == 'recurso_arribo': new_columns[col] = 'Recurso Arribo'
                elif col == 'comuna_calculada': new_columns[col] = 'comuna_calculada' # Mantener
                elif col == 'dni_categorizado': new_columns[col] = 'DNI_Categorizado' # Mantener
                else:
                    new_columns[col] = col.title().replace('_', ' ')
            
            df.rename(columns=new_columns, inplace=True)
        
        # Convert date columns
        date_columns = ['Fecha Inicio', 'Fecha Fin', 'Recurso Fecha Liberado', 
                       'Recurso Fecha asignacion', 'Recurso Arribo']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        print(f"✅ Descarga exitosa desde Neon: {len(df):,} registros")
        return df
        
    except Exception as e:
        if "does not exist" in str(e).lower():
            print(f"⚠️ Tabla '{table_name}' no existe en Neon. Retornando DataFrame vacío (primera carga).")
            return pd.DataFrame()
        else:
            print(f"❌ Error descargando desde Neon: {e}")
            raise e
    finally:
        engine.dispose()


def get_max_date_from_neon(table_name='historico_limpio', date_column='Fecha Inicio'):
    """
    Gets the maximum date (watermark) from Neon PostgreSQL table.
    Handles column name mapping (Fecha Inicio -> fecha_inicio).
    """
    # Mapeo de columna para la query SQL
    db_col_name = to_snake_case(date_column)
    
    print(f"🔍 Obteniendo watermark desde Neon: tabla '{table_name}', columna '{db_col_name}'...")
    
    try:
        engine = get_neon_engine()
        
        # Usar el nombre de columna mapeado en la query
        query = f'''
        SELECT MAX("{db_col_name}") as max_fecha
        FROM "{table_name}"
        '''
        
        result = pd.read_sql(query, engine)
        max_fecha = result['max_fecha'][0]
        
        if pd.isna(max_fecha):
            print(f"⚠️ Tabla '{table_name}' está vacía. No hay watermark.")
            return None
        
        max_fecha = pd.to_datetime(max_fecha)
        print(f"✅ Watermark encontrado: {max_fecha}")
        return max_fecha
        
    except Exception as e:
        if "does not exist" in str(e).lower():
            print(f"⚠️ Tabla '{table_name}' no existe. No hay watermark (primera carga).")
            return None
        # Fallback: intentar con el nombre original por si acaso
        elif 'column' in str(e).lower() and 'does not exist' in str(e).lower():
             print(f"⚠️ Columna '{db_col_name}' no encontrada. Probando '{date_column}'...")
             try:
                 query = f'SELECT MAX("{date_column}") as max_fecha FROM "{table_name}"'
                 result = pd.read_sql(query, engine)
                 return pd.to_datetime(result['max_fecha'][0])
             except:
                 pass
                 
        print(f"⚠️ Error obteniendo watermark desde Neon: {e}")
        return None
    finally:
        engine.dispose()


def upload_to_neon_incremental(df, table_name, if_exists='append'):
    """
    Uploads a DataFrame to Neon PostgreSQL.
    Renames columns to snake_case before uploading to match DB schema.
    """
    print(f"⬆️ Iniciando carga a Neon PostgreSQL: tabla '{table_name}' (mode: {if_exists})...")
    
    try:
        engine = get_neon_engine()
        
        # Crear copia para no modificar el DF original que se usa en el resto del script
        df_to_upload = df.copy()
        
        # Renombrar columnas a snake_case
        df_to_upload.columns = [to_snake_case(col) for col in df_to_upload.columns]
        
        # Excepciones específicas si es necesario
        # Por ejemplo, si 'comuna_calculada' ya es snake_case, to_snake_case lo deja igual, OK.
        
        df_to_upload.to_sql(
            table_name, 
            engine, 
            if_exists=if_exists, 
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"✅ Carga a Neon PostgreSQL exitosa: {len(df)} registros")
        
    except Exception as e:
        print(f"❌ Error subiendo a Neon: {e}")
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
            MIN("fecha_inicio") as min_fecha,
            MAX("fecha_inicio") as max_fecha
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
