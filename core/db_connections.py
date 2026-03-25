"""
Database connection utilities for Neon PostgreSQL.
"""

import os
import time
import pandas as pd
from sqlalchemy import create_engine, text

import sys
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


def get_neon_connection_string():
    conn_str = os.getenv('DATABASE_URL')
    if conn_str:
        print("✅ Using DATABASE_URL from environment variable")
        return conn_str

    import json
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        creds_path = os.path.join(base_dir, 'credentials.json')
        if os.path.exists(creds_path):
            with open(creds_path, 'r') as f:
                creds = json.load(f)
                if 'DATABASE_URL' in creds:
                    print("✅ Using DATABASE_URL from credentials.json")
                    # Sacar channel_binding si est presente - causa SSL EOF en psycopg2
                    url = creds['DATABASE_URL']
                    url = url.replace('&channel_binding=require', '')
                    url = url.replace('?channel_binding=require&', '?')
                    url = url.replace('?channel_binding=require', '')
                    return url
    except Exception as e:
        print(f"⚠️ Warning: Could not read credentials.json for DATABASE_URL: {e}")

    return (
        "postgresql://neondb_owner:npg_5VpD8FQecTGn"
        "@ep-fancy-bar-a8pbran5-pooler.eastus2.azure.neon.tech"
        "/neondb?sslmode=require"
    )


def get_neon_engine():
    """
    Engine con pool_pre_ping=True para que SQLAlchemy verifique la conexin
    antes de usarla y reconecte automticamente si Neon la cerr.
    pool_size=1 y max_overflow=0 para no acumular conexiones en Neon free tier.
    """
    conn_str = get_neon_connection_string()
    return create_engine(
        conn_str,
        pool_pre_ping=True,       # verifica conexin antes de cada uso
        pool_size=1,              # solo 1 conexin en el pool
        max_overflow=0,           # sin conexiones extra
        pool_recycle=300,         # reciclar conexiones cada 5 min
        connect_args={
            "connect_timeout": 10,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
    )


def _ejecutar_con_retry(fn, max_intentos=3, espera_base=5):
    """
    Ejecuta fn() con retry y backoff ante errores SSL/conexin de Neon.
    fn debe ser un callable sin argumentos que retorna el resultado deseado.
    """
    ultimo_error = None
    for intento in range(1, max_intentos + 1):
        try:
            return fn()
        except Exception as exc:
            ultimo_error = exc
            es_ssl = any(k in str(exc).lower() for k in [
                'ssl', 'eof', 'connection', 'timeout', 'closed unexpectedly'
            ])
            if es_ssl and intento < max_intentos:
                espera = espera_base * intento
                print(f"   ⚠️  Intento {intento}/{max_intentos} fallido (SSL/conexin). "
                      f"Reintentando en {espera}s...")
                time.sleep(espera)
            else:
                raise
    raise ultimo_error


def to_snake_case(col_name):
    return col_name.strip().lower().replace(' ', '_')


def to_title_case(col_name):
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
        'subtipo': 'SubTipo',
        'comuna_calculada': 'comuna_calculada',
        'dni_categorizado': 'dni_categorizado'
    }
    return MAPPING.get(col_name, col_name.title().replace('_', ' '))


# ==============================================================================
# UTILIDADES MANUALES / LEGADO
# (No se usan en el ETL incremental activo, conservadas para mantenimiento)
# ==============================================================================

def download_from_neon(table_name='2025_historico_limpio'):
    print(f"[DOWNLOAD] Descargando histrico desde Neon PostgreSQL: tabla '{table_name}'...")

    def _fetch():
        engine = get_neon_engine()
        try:
            with engine.connect() as conn:
                df = pd.read_sql(f'SELECT * FROM {table_name}', conn)
            return df
        finally:
            engine.dispose()

    try:
        df = _ejecutar_con_retry(_fetch)

        if 'fecha_inicio' in df.columns:
            print(" Detectado snake_case en Neon. Renombrando columnas...")
            new_columns = {}
            for col in df.columns:
                if col == 'fecha_inicio': new_columns[col] = 'Fecha Inicio'
                elif col == 'fecha_fin': new_columns[col] = 'Fecha Fin'
                elif col == 'id_suceso': new_columns[col] = 'Id Suceso'
                elif col == 'persona_dni': new_columns[col] = 'Persona DNI'
                elif col == 'recurso_fecha_liberado': new_columns[col] = 'Recurso Fecha Liberado'
                elif col == 'recurso_fecha_asignacion': new_columns[col] = 'Recurso Fecha asignacion'
                elif col == 'recurso_arribo': new_columns[col] = 'Recurso Arribo'
                elif col == 'comuna_calculada': new_columns[col] = 'comuna_calculada'
                elif col == 'dni_categorizado': new_columns[col] = 'dni_categorizado'
                else: new_columns[col] = col.title().replace('_', ' ')
            df.rename(columns=new_columns, inplace=True)

        for col in ['Fecha Inicio', 'Fecha Fin', 'Recurso Fecha Liberado',
                    'Recurso Fecha asignacion', 'Recurso Arribo']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        print(f"✅ Descarga exitosa desde Neon: {len(df):,} registros")
        return df

    except Exception as e:
        if "does not exist" in str(e).lower():
            print(f"⚠️ Tabla '{table_name}' no existe en Neon. Retornando DataFrame vaco.")
            return pd.DataFrame()
        print(f"❌ Error descargando desde Neon: {e}")
        raise


def get_max_date_from_neon(table_name='historico_limpio', date_column='Fecha Inicio'):
    print(f"🔍 Obteniendo watermark desde Neon: tabla '{table_name}', columna '{date_column}'...")

    def _fetch():
        engine = get_neon_engine()
        try:
            q = f'SELECT MAX("{date_column}") as max_fecha FROM "{table_name}"'
            with engine.connect() as conn:
                result = pd.read_sql(q, conn)
            return result
        finally:
            engine.dispose()

    try:
        result = _ejecutar_con_retry(_fetch)
        max_fecha = result['max_fecha'][0]
        if pd.isna(max_fecha):
            print(f"⚠️ Tabla '{table_name}' est vaca. No hay watermark.")
            return None
        max_fecha = pd.to_datetime(max_fecha)
        print(f"✅ Watermark encontrado: {max_fecha}")
        return max_fecha

    except Exception as e:
        if "does not exist" in str(e).lower():
            print(f"⚠️ Tabla '{table_name}' no existe. No hay watermark (primera carga).")
            return None
        print(f"⚠️ Error obteniendo watermark desde Neon: {e}")
        return None


def get_table_stats(table_name='historico_limpio'):
    def _fetch():
        engine = get_neon_engine()
        try:
            q = f"""
            SELECT
                COUNT(*) as total_records,
                MIN("Fecha Inicio") as min_fecha,
                MAX("Fecha Inicio") as max_fecha
            FROM {table_name}
            """
            with engine.connect() as conn:
                return pd.read_sql(q, conn)
        finally:
            engine.dispose()

    try:
        result = _ejecutar_con_retry(_fetch)
        return {
            'total_records': result['total_records'][0],
            'min_fecha':     result['min_fecha'][0],
            'max_fecha':     result['max_fecha'][0],
        }
    except Exception as e:
        print(f"[WARN] Error obteniendo estadsticas de {table_name}: {e}")
        return None


def upload_to_neon_incremental(df, table_name, if_exists='append'):
    print(f"[UPLOAD] Iniciando carga a Neon PostgreSQL: tabla '{table_name}' (mode: {if_exists})...")

    def _upload():
        engine = get_neon_engine()
        try:
            df_up = df.copy()
            df_up.columns = [to_snake_case(col) for col in df_up.columns]
            df_up.to_sql(table_name, engine, if_exists=if_exists,
                         index=False, method='multi', chunksize=1000)
        finally:
            engine.dispose()

    try:
        _ejecutar_con_retry(_upload)
        print(f"[OK] Carga a Neon PostgreSQL exitosa: {len(df)} registros")
    except Exception as e:
        print(f"[ERROR] Error subiendo a Neon: {e}")
        raise


def replace_table_in_neon(df, table_name):
    print(f">> Iniciando REPLACE completo en Neon: tabla '{table_name}' ({len(df):,} registros)...")

    def _replace():
        engine = get_neon_engine()
        try:
            df_up = df.copy()
            df_up.columns = [to_snake_case(col) for col in df_up.columns]
            df_up.to_sql(table_name, engine, if_exists='replace',
                         index=False, method='multi', chunksize=500)
        finally:
            engine.dispose()

    try:
        _ejecutar_con_retry(_replace)
        print(f"   OK - Replace completo exitoso: {len(df):,} registros cargados.")
    except Exception as e:
        print(f"   ERROR en replace_table_in_neon: {e}")
        raise


def get_dni_history(table_name='historico_limpio'):
    print(f"[SEARCH] Recuperando historial de DNIs desde Neon (tabla '{table_name}')...")

    def _fetch():
        engine = get_neon_engine()
        try:
            q = f"""
            SELECT "DNI_categorizado", comuna_calculada
            FROM (
                SELECT
                    "DNI_categorizado",
                    comuna_calculada,
                    ROW_NUMBER() OVER(
                        PARTITION BY "DNI_categorizado"
                        ORDER BY "Fecha Inicio" DESC
                    ) as rn
                FROM "{table_name}"
                WHERE "DNI_categorizado" IS NOT NULL
                  AND "DNI_categorizado" NOT IN (
                      'NO BRINDO/NO VISIBLE', 'NO BRINDO', 'NO VISIBLE', 'S/D', 'CONTACTO EXTRANJERO'
                  )
            ) t
            WHERE rn = 1
            """
            with engine.connect() as conn:
                return pd.read_sql(q, conn)
        finally:
            engine.dispose()

    try:
        df_hist = _ejecutar_con_retry(_fetch)
        if df_hist.empty:
            print("[WARN] No hay historial previo de DNIs en la base de datos.")
            return {}
        history_dict = dict(zip(
            df_hist['DNI_categorizado'].astype(str),
            df_hist['comuna_calculada']
        ))
        print(f"[OK] Historial recuperado: {len(history_dict):,} DNIs nicos encontrados.")
        return history_dict
    except Exception as e:
        print(f"[WARN] Error recuperando historial de DNIs: {e}")
        return {}


def update_apariciones_en_neon(clave_a_apariciones, table_name='"2025_historico_limpio_(2)"'):
    print(f" Actualizando columna 'apariciones' masivamente en Neon (tabla '{table_name}')...")
    if not clave_a_apariciones:
        print("[WARN] El diccionario de apariciones est vaco. Saltando UPDATE.")
        return

    def _update():
        import sqlalchemy
        engine = get_neon_engine()
        try:
            with engine.begin() as conn:
                conn.execute(sqlalchemy.text(
                    f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS apariciones INTEGER'
                ))
                dni_rows, nombre_rows = [], []
                for (tipo, valor), count in clave_a_apariciones.items():
                    if tipo == 'dni':
                        dni_rows.append(f"('{valor.replace(chr(39), chr(39)*2)}', {int(count)})")
                    else:
                        partes = valor.split('|', 1)
                        nombre   = (partes[0] if partes else '').replace("'", "''")
                        apellido = (partes[1] if len(partes) > 1 else '').replace("'", "''")
                        nombre_rows.append(f"('{nombre}', '{apellido}', {int(count)})")

                if dni_rows:
                    conn.execute(sqlalchemy.text(f"""
                        UPDATE {table_name} AS t SET apariciones = v.apariciones
                        FROM (VALUES {', '.join(dni_rows)}) AS v(dni_val, apariciones)
                        WHERE t.dni_categorizado = v.dni_val
                          AND t.dni_categorizado NOT IN (
                              'NO BRINDO/NO VISIBLE', 'NO BRINDO', 'NO VISIBLE', 'S/D')
                    """))
                    print(f"   [OK] UPDATE por DNI completado ({len(dni_rows)} claves nicas)")

                if nombre_rows:
                    conn.execute(sqlalchemy.text(f"""
                        UPDATE {table_name} AS t SET apariciones = v.apariciones
                        FROM (VALUES {', '.join(nombre_rows)}) AS v(nombre_val, apellido_val, apariciones)
                        WHERE UPPER(TRIM(t.persona_nombre)) = v.nombre_val
                          AND UPPER(TRIM(t.persona_apellido)) = v.apellido_val
                          AND t.dni_categorizado IN (
                              'NO BRINDO/NO VISIBLE', 'NO BRINDO', 'NO VISIBLE', 'S/D')
                    """))
                    print(f"   [OK] UPDATE por Nombre+Apellido completado ({len(nombre_rows)} claves nicas)")
        finally:
            engine.dispose()

    try:
        _ejecutar_con_retry(_update)
        print("[OK] Columna 'apariciones' actualizada exitosamente en Neon.")
    except Exception as e:
        print(f"[ERROR] Error en update masivo de apariciones: {e}")
        raise