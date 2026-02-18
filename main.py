import os
import sys
# Importar funciones desde los módulos refactorizados
from core.drive_manager import get_drive_service, download_file_as_bytes, get_max_date_from_parquet
from core.db_connections import get_table_stats, get_max_date_from_neon
from data_processor import procesar_datos

# --- CONFIGURACIÓN DE CARPETAS (IDs ACTUALIZADOS) ---

# 1. CARPETA DE ENTRADA (01_insumos): Donde están tus .xls semanales
# TEMPORALMENTE DESHABILITADO: Ya no se busca el Excel en Drive/Gmail
# INPUT_FOLDER_ID = '14kWGqDj-Q_TOl2-F9FqocI9H_SeL_6Ba' 

# 2. CARPETA DE BASE DE DATOS (02_base_datos): Donde vive el parquet y el histórico
DB_FOLDER_ID = '1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t'

# --- CARGA LOCAL TEMPORAL ---
# El archivo Excel debe estar en la misma carpeta que este script
LOCAL_EXCEL_FILENAME = "febrero1.xlsx"  # Cambia el nombre según tu archivo

def main():
    print("🏁 Iniciando proceso de captura (Master Flow: Drive Parquet -> Neon)...")
    
    # 1. Autenticación con Google Drive
    try:
        service = get_drive_service()
    except Exception as e:
        print(f"❌ Error de autenticación: {e}")
        return

    # 2. Búsqueda del Excel en la carpeta de Drive (01_insumos)
    INPUT_FOLDER_ID = '14kWGqDj-Q_TOl2-F9FqocI9H_SeL_6Ba' 
    print(f"🔎 Buscando reportes en Drive: {INPUT_FOLDER_ID}...")
    query = (
        f"'{INPUT_FOLDER_ID}' in parents "
        "and (mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' "
        "or mimeType = 'application/vnd.ms-excel') "
        "and trashed = false"
    )
    results = service.files().list(
        q=query, 
        orderBy='createdTime desc', 
        pageSize=1, 
        fields="files(id, name, createdTime)"
    ).execute()
    files = results.get('files', [])
    
    if not files:
        print("⚠️ No se encontró ningún archivo Excel en '01_insumos'.")
        return
        
    archivo_excel = files[0]
    print(f"📄 Archivo detectado: {archivo_excel['name']} (ID: {archivo_excel['id']})")

    # 3. Descarga del Excel desde Drive a memoria
    try:
        excel_bytes = download_file_as_bytes(service, archivo_excel['id'])
        print("✅ Descarga del Excel completada.")
    except Exception as e:
        print(f"❌ Error descargando archivo: {e}")
        return

    # 4. Detectar watermark desde Parquet (Diagram Step 1)
    # FILE_NAME_PARQUET = '2025_historico_limpio.parquet'
    DB_FOLDER_ID = '1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t'
    print(f"\n🔍 1. Detectando watermark desde Parquet en Drive...")
    watermark = get_max_date_from_parquet(service, "2025_historico_limpio.parquet", DB_FOLDER_ID)
    
    if watermark:
        print(f"✅ Watermark detectado: {watermark}")
    else:
        print("⚠️ No se detectó watermark. Se procesará todo.")

    # 5. Enviar al Procesador (Diagram Step 2)
    try:
        print(f"\n🚀 2. Enviando a data_processor.py con watermark: {watermark}")
        procesar_datos(excel_bytes, DB_FOLDER_ID, watermark=watermark)
        print("\n🚀 Ciclo completo finalizado exitosamente.")
    except Exception as e:
        print(f"❌ Error durante el procesamiento: {e}")
        raise e

if __name__ == '__main__':
    main()
