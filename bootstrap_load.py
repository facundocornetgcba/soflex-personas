"""
Bootstrap Load Script - Full Replace Mode

This script is used for:
- Initial database setup
- Emergency full reloads
- Data corruption recovery
- Schema changes requiring full refresh

It downloads the complete historical clean data from Drive and performs
a full REPLACE load to Neon PostgreSQL.

Usage:
    python bootstrap_load.py
"""

import sys
from core.drive_manager import get_drive_service, download_parquet_as_df
from core.db_connections import upload_to_neon_incremental


# Google Drive folder configuration
DB_FOLDER_ID = '1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t'  # 02_base_datos folder
HISTORICO_LIMPIO = '2025_historico_limpio.parquet'
TABLE_NAME = 'historico_limpio'


def bootstrap_full_load():
    """
    Performs a full replace load of the historical clean data to Neon PostgreSQL.
    
    This will:
    1. Download the complete historical parquet from Drive
    2. Replace the entire table in Neon with this data
    3. Print statistics about the load
    """
    print("=" * 60)
    print("🔄 BOOTSTRAP LOAD - FULL REPLACE MODE")
    print("=" * 60)
    print()
    
    try:
        # Step 1: Authenticate to Drive
        print("🔐 Autenticando con Google Drive...")
        service = get_drive_service()
        print("✅ Autenticación exitosa")
        print()
        
        # Step 2: Download historical clean data
        print(f"📥 Descargando histórico limpio desde Drive...")
        df = download_parquet_as_df(service, HISTORICO_LIMPIO, DB_FOLDER_ID)
        
        if df.empty:
            print("❌ ERROR: El archivo histórico está vacío o no existe")
            print(f"   Verificar que '{HISTORICO_LIMPIO}' existe en la carpeta {DB_FOLDER_ID}")
            sys.exit(1)
        
        print(f"✅ Descarga completa: {len(df):,} registros")
        print()
        
        # Step 3: Show data preview
        if 'Fecha Inicio' in df.columns:
            fecha_min = df['Fecha Inicio'].min()
            fecha_max = df['Fecha Inicio'].max()
            print(f"📅 Rango de fechas: {fecha_min} a {fecha_max}")
        
        if 'comuna_calculada' in df.columns:
            comunas = df['comuna_calculada'].nunique()
            print(f"🗺️  Comunas únicas: {comunas}")
        
        print()
        
        # Step 4: Confirm before proceeding
        print("⚠️  ADVERTENCIA: Esta operación reemplazará TODA la tabla en Neon PostgreSQL")
        confirm = input("¿Desea continuar? (escriba 'SI' para confirmar): ")
        
        if confirm.upper() != 'SI':
            print("❌ Operación cancelada por el usuario")
            sys.exit(0)
        
        print()
        
        # Step 5: Upload to Neon with REPLACE mode
        print(f"🚀 Cargando datos a Neon PostgreSQL (tabla: {TABLE_NAME})...")
        upload_to_neon_incremental(df, TABLE_NAME, if_exists='replace')
        
        print()
        print("=" * 60)
        print("✅ BOOTSTRAP COMPLETO")
        print("=" * 60)
        print(f"   Total de registros cargados: {len(df):,}")
        print(f"   Tabla: {TABLE_NAME}")
        print(f"   Modo: REPLACE (reemplazo completo)")
        print()
        print("💡 El sistema ahora está listo para cargas incrementales con main.py")
        
    except KeyboardInterrupt:
        print("\\n\\n❌ Operación cancelada por el usuario")
        sys.exit(1)
        
    except Exception as e:
        print(f"\\n\\n❌ ERROR durante el bootstrap: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    bootstrap_full_load()
