"""
Bootstrap script to load existing Parquet files from Google Drive to Neon PostgreSQL.
This ensures Neon starts with the same data as Drive.
"""

import pandas as pd
from core.drive_manager import get_drive_service, download_parquet_as_df
from core.db_connections import upload_to_neon_incremental

# Drive Folder ID (02_base_datos)
DB_FOLDER_ID = '1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t'

def bootstrap():
    service = get_drive_service()
    
    # 1. Migrar Histórico Crudo
    print("📦 Iniciando migración de Histórico Crudo (historico)...")
    try:
        df_crudo = download_parquet_as_df(service, '2025_historico_v2.parquet', DB_FOLDER_ID)
        if not df_crudo.empty:
            upload_to_neon_incremental(df_crudo, 'historico', if_exists='replace')
            print(f"✅ Histórico Crudo migrado: {len(df_crudo):,} registros")
        else:
            print("⚠️ Parquet crudo vacío en Drive.")
    except Exception as e:
        print(f"❌ Error migrando crudo: {e}")

    # 2. Migrar Histórico Limpio
    print("\n📦 Iniciando migración de Histórico Limpio...")
    try:
        df_limpio = download_parquet_as_df(service, '2025_historico_limpio.parquet', DB_FOLDER_ID)
        if not df_limpio.empty:
            upload_to_neon_incremental(df_limpio, 'historico_limpio', if_exists='replace')
            print(f"✅ Histórico Limpio migrado: {len(df_limpio):,} registros")
        else:
            print("⚠️ Parquet limpio vacío en Drive.")
    except Exception as e:
        print(f"❌ Error migrando limpio: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando Bootstrap de Datos Drive -> Neon...")
    bootstrap()
    print("\n🎉 Bootstrap finalizado.")
