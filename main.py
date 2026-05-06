#!/usr/bin/env python3
"""
main.py  -  ETL incremental semanal
Corre cada semana (manualmente o via GitHub Actions).

Flujo:
  1. Busca el Excel ms reciente en la carpeta 01_insumos de Drive
  2. Detecta el watermark (fecha mxima en Neon)
  3. Filtra solo registros nuevos y los procesa
  4. Append a Neon + actualiza parquet backup en Drive
"""

from core.db_connections import get_max_date_from_neon, get_table_stats, get_neon_engine
from core.drive_manager   import (
    download_file_as_bytes, get_drive_service,
    download_parquet_as_df, upload_df_as_parquet, get_max_date_from_parquet,
)
from core.gmail_manager   import get_latest_excel_from_gmail
from data_processor        import procesar_datos

import sys
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        # Fallback if reconfigure is not available
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

#  Configuracion 

INPUT_FOLDER_ID = "14kWGqDj-Q_TOl2-F9FqocI9H_SeL_6Ba"  # 01_insumos
DB_FOLDER_ID    = "1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t"  # 02_base_datos
TABLE_NEON      = "historico_limpio"
COL_FECHA       = "Fecha Inicio"


FILE_PARQUET = "2025_historico_limpio.parquet"


def _sincronizar_parquet_desde_neon(service, parquet_max, folder_id):
    """
    Descarga desde Neon los registros posteriores a parquet_max
    y los appendea al parquet de Drive.
    Solo se llama cuando Neon tiene datos más recientes que el parquet.
    """
    import pandas as pd

    print(f"\n⚠️  Parquet desincronizado (max: {parquet_max}). Sincronizando desde Neon...")
    engine = get_neon_engine()
    try:
        q = f"""
            SELECT * FROM historico_limpio
            WHERE "Fecha Inicio" > :fecha
            ORDER BY "Fecha Inicio"
        """
        from sqlalchemy import text as sa_text
        with engine.connect() as conn:
            df_delta = pd.read_sql(sa_text(q), conn, params={"fecha": parquet_max})
    except Exception as exc:
        print(f"   ❌ Error consultando Neon para sync: {exc}")
        return
    finally:
        engine.dispose()

    if df_delta.empty:
        print("   ✅ Neon no tiene datos adicionales. Parquet ya sincronizado.")
        return

    print(f"   📥 {len(df_delta):,} registros a appendear al parquet...")
    df_prev = download_parquet_as_df(service, FILE_PARQUET, folder_id)
    if df_prev is not None and not df_prev.empty:
        df_prev["Fecha Inicio"] = pd.to_datetime(df_prev["Fecha Inicio"], errors="coerce")
        # Alinear dtypes del delta con el parquet existente para evitar conflictos en concat
        for col in df_prev.columns:
            if col not in df_delta.columns:
                continue
            try:
                if pd.api.types.is_datetime64_any_dtype(df_prev[col].dtype):
                    df_delta[col] = pd.to_datetime(df_delta[col], errors="coerce")
                elif pd.api.types.is_integer_dtype(df_prev[col].dtype):
                    df_delta[col] = pd.to_numeric(df_delta[col], errors="coerce").astype("Int64")
                elif pd.api.types.is_float_dtype(df_prev[col].dtype):
                    df_delta[col] = pd.to_numeric(df_delta[col], errors="coerce")
                else:
                    df_delta[col] = df_delta[col].astype(df_prev[col].dtype)
            except Exception:
                pass
        df_completo = pd.concat([df_prev, df_delta], ignore_index=True)
    else:
        df_completo = df_delta

    upload_df_as_parquet(service, df_completo, FILE_PARQUET, folder_id)
    print(f"   ✅ Parquet sincronizado: {len(df_completo):,} registros totales.")


def main():
    print("=" * 60)
    print("  ETL Incremental Semanal")
    print("=" * 60)

    # 1. Autenticacion Drive
    print("\n🔑 Autenticando con Google Drive...")
    try:
        service = get_drive_service()
        print("   ✅ OK")
    except Exception as exc:
        print(f"   ❌ Error: {exc}")
        raise

    # 2-3. DESCARGAR EXCEL DESDE GMAIL
    print("\n⬇️  Buscando adjunto en Gmail (Informe BAP Personas)...")
    try:
        excel_bytes = get_latest_excel_from_gmail()
        print(f"   ✅ OK ({len(excel_bytes):,} bytes)")
    except Exception as exc:
        print(f"   ❌ Error: {exc}")
        raise

    # 4. Watermark desde Neon
    print(f"\n🔍 Watermark en Neon ({TABLE_NEON})...")
    watermark = get_max_date_from_neon(TABLE_NEON, COL_FECHA)

    if watermark:
        print(f"   ✅ {watermark}  ->  solo registros posteriores a esta fecha")
    else:
        print("   ⚠️  Tabla vaca -> se procesa todo (primera carga)")

    # Info actual de Neon (informativo, no bloquea el ETL)
    try:
        stats = get_table_stats(TABLE_NEON)
        if stats:
            print(f"\n📊 Neon actual: {stats['total_records']:,} registros  "
                  f"({stats['min_fecha']} -> {stats['max_fecha']})")
    except Exception:
        pass  # No crtico: sigue adelante aunque no se puedan obtener stats

    # 5. ETL incremental
    print()
    try:
        resultado = procesar_datos(excel_bytes, DB_FOLDER_ID, watermark=watermark)
    except Exception as exc:
        print(f"\n❌ Error en el procesamiento: {exc}")
        raise

    if resultado is None:
        print("\n✅ Sin datos nuevos. Neon no fue modificado.")
        # Verificar si el parquet quedó desincronizado de una corrida anterior fallida
        if watermark:
            parquet_max = get_max_date_from_parquet(service, FILE_PARQUET, DB_FOLDER_ID)
            if parquet_max is None or parquet_max < watermark:
                _sincronizar_parquet_desde_neon(service, parquet_max, DB_FOLDER_ID)
            else:
                print("✅ Parquet ya sincronizado con Neon.")
    else:
        print(f"\n✅ {len(resultado):,} registros nuevos cargados en Neon.")


if __name__ == "__main__":
    main()