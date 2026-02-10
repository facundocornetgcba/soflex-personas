"""
Google Drive operations utilities.

This module centralizes all Google Drive API interactions including:
- Authentication
- File download/upload
- Parquet operations
- Watermark detection
"""

import os
import io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload


# Scopes for Google Drive
SCOPES = [
    'https://www.googleapis.com/auth/drive',
]


def get_credentials():
    """
    Obtiene las credenciales para usar en Drive.
    
    Priority:
    1. Environment variable GOOGLE_APPLICATION_CREDENTIALS (GitHub Actions)
    2. Local credentials.json file
    
    Returns:
        google.oauth2.service_account.Credentials: Service account credentials
    """
    # Prioridad: Variable de entorno (GitHub Actions) > Archivo local
    creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')
    
    # Crea las credenciales con los scopes necesarios
    creds = service_account.Credentials.from_service_account_file(
        creds_path, 
        scopes=SCOPES
    )
    return creds


def get_drive_service():
    """
    Autentica y devuelve el servicio de Drive usando las credenciales compartidas.
    
    Returns:
        googleapiclient.discovery.Resource: Authenticated Drive service
    """
    creds = get_credentials()
    return build('drive', 'v3', credentials=creds)


def download_file_as_bytes(service, file_id):
    """
    Descarga un archivo cualquiera de Drive y devuelve sus bytes.
    
    Args:
        service: Authenticated Drive service
        file_id (str): Google Drive file ID
        
    Returns:
        bytes: File content as bytes
    """
    print(f"⬇️ Descargando archivo ID: {file_id}...")
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()


def download_parquet_as_df(service, file_name, folder_id):
    """
    Busca y descarga un parquet de Drive a un DataFrame.
    
    Args:
        service: Authenticated Drive service
        file_name (str): Name of the parquet file
        folder_id (str): Google Drive folder ID
        
    Returns:
        pd.DataFrame: DataFrame loaded from parquet, empty if file not found
    """
    print(f"⬇️ Buscando '{file_name}' en Drive...")
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    
    if not files:
        print(f"⚠️ Archivo {file_name} no encontrado. Se creará uno nuevo.")
        return pd.DataFrame() 

    file_id = files[0]['id']
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    fh.seek(0)
    return pd.read_parquet(fh)


def upload_df_as_parquet(service, df, file_name, folder_id):
    """
    Sube un DataFrame como parquet a Drive (sobreescribe o crea).
    
    Args:
        service: Authenticated Drive service
        df (pd.DataFrame): DataFrame to upload
        file_name (str): Name for the parquet file
        folder_id (str): Google Drive folder ID
    """
    print(f"⬆️ Subiendo '{file_name}' a Drive...")
    fh = io.BytesIO()
    df.to_parquet(fh, index=False, engine='pyarrow', compression='snappy')
    fh.seek(0)
    
    media = MediaIoBaseUpload(fh, mimetype='application/octet-stream', resumable=True)
    
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])

    if files:
        file_id = files[0]['id']
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"✅ {file_name} actualizado en Drive.")
    else:
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        service.files().create(body=file_metadata, media_body=media).execute()
        print(f"✅ {file_name} creado en Drive.")


def get_max_date_from_parquet(service, file_name, folder_id, date_column='Fecha Inicio'):
    """
    Lee el parquet del histórico limpio y retorna el MAX(date_column) como watermark.
    
    This is the key function for incremental loading - it detects the latest date
    in the historical data so we only process newer records.
    
    Args:
        service: Authenticated Drive service
        file_name (str): Name of the parquet file
        folder_id (str): Google Drive folder ID
        date_column (str): Name of the date column to use as watermark
        
    Returns:
        pd.Timestamp or None: Maximum date found, or None if file doesn't exist or is empty
    """
    try:
        df = download_parquet_as_df(service, file_name, folder_id)
        
        if df.empty:
            print(f"⚠️ Archivo {file_name} vacío o no existe. No hay watermark.")
            return None
        
        if date_column not in df.columns:
            print(f"❌ Columna '{date_column}' no encontrada en {file_name}")
            return None
        
        # Ensure the column is datetime
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        
        max_date = df[date_column].max()
        
        if pd.isna(max_date):
            print(f"⚠️ No se pudo determinar fecha máxima en {file_name}")
            return None
        
        print(f"📅 Watermark detectado: {max_date}")
        return max_date
        
    except Exception as e:
        print(f"⚠️ Error obteniendo watermark de {file_name}: {e}")
        return None
