import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import os
import io
import re
import gc
import zipfile
import fiona
from sqlalchemy import create_engine

# Importar desde módulos core
from core.drive_manager import (
    get_drive_service, 
    download_parquet_as_df, 
    upload_df_as_parquet
)
from core.db_connections import upload_to_neon_incremental
from core.transformations import (
    limpiar_texto,
    limpiar_texto_cierre,
    limpiar_y_categorizar_dni_v3,
    mapear_categoria_con_reglas,
    obtener_niveles
)

def get_drive_service():
    """Autentica y devuelve el servicio de Drive usando las credenciales compartidas."""
    creds = get_credentials()
    return build('drive', 'v3', credentials=creds)

def download_file_as_bytes(service, file_id):
    """Descarga un archivo cualquiera de Drive y devuelve sus bytes."""
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
    """Busca y descarga un parquet de Drive a un DataFrame."""
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
    """Sube un DataFrame como parquet a Drive (sobreescribe o crea)."""
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

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """Sube el DataFrame a BigQuery reemplazando la tabla existente."""
    destination_table = f"{dataset_id}.{table_id}"
    print(f"⬆️ Iniciando carga a BigQuery: {destination_table} en proyecto {project_id}...")
    
    try:
        creds = get_credentials()
        # if_exists='replace' es CRÍTICO para mantener la consistencia de tu lógica de históricos
        df.to_gbq(
            destination_table, 
            project_id=project_id, 
            if_exists='replace',
            credentials=creds,
            progress_bar=False
        )
        print("✅ Carga a BigQuery exitosa.")
    except Exception as e:
        print(f"❌ Error subiendo a BigQuery: {e}")

def upload_to_neon(df, table_name):
    """Sube el DataFrame a Neon PostgreSQL reemplazando la tabla existente."""
    print(f"⬆️ Iniciando carga a Neon PostgreSQL: tabla '{table_name}'...")
    
    # Connection string provided by user
    conn_str = "postgresql://neondb_owner:npg_3X7LoQzdmWhH@ep-square-paper-a89hpeb2-pooler.eastus2.azure.neon.tech/neondb?sslmode=require&channel_binding=require"
    
    try:
        engine = create_engine(conn_str)
        # index=False para no subir el índice de pandas como columna
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print("✅ Carga a Neon PostgreSQL exitosa.")
    except Exception as e:
        print(f"❌ Error subiendo a Neon: {e}")
        # Re-raise para que el proceso falle visiblemente si no se puede guardar
        raise e

# ==========================================
# FUNCIONES DE LIMPIEZA (TU LÓGICA)
# ==========================================

def limpiar_texto(nombre):
    if pd.isna(nombre): return None
    nombre = str(nombre).upper()
    nombre = ''.join(c for c in unicodedata.normalize('NFD', nombre) if unicodedata.category(c) != 'Mn')
    nombre = re.sub(r'[-.,]', ' ', nombre)
    nombre = re.sub(r'[^A-Z ]', '', nombre)
    nombre = re.sub(r'\s+', ' ', nombre).strip()
    return nombre if nombre else None

def limpiar_texto_cierre(s):
    if pd.isna(s): return ""
    s = str(s).lower().strip()
    s = unidecode.unidecode(s)
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"[^a-z0-9áéíóúüñ\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

# --- PATRONES REGEX PARA DNI ---
PATRON_EXTRANJERO = re.compile(r'(extranjero|paraguay|venezol|colombian|uruguay|brasil|chilen|peruano|mexican|español|dominican|dominicana|pasaporte|c\.?d\.?[ie]:?|rnm|cedula|ciudadano\s+extranjero)', flags=re.IGNORECASE)
PATRON_NO_BRINDO_GENERICOS = re.compile(r'(no\s*brind|no\s*bri[nm]d|no\s*aporta|no\s*aporto|no\s*indica|no\s*sabe|no\s*recuerda|no\s*recuerd|no\s*tiene|nunca\s*tuvo|sin\s*dni|sin\s*dato|sin\s*inform|ilegible|invisible|no\s*visible|exhib|no\s*lo\s*sabe|menor\s*de\s*edad)', flags=re.IGNORECASE)
PATRON_NO_BRINDO_SIMBOLOS = re.compile(r'^[xX\*\-\.]+$', flags=re.IGNORECASE)
PATRON_LETRAS_CORTAS = re.compile(r'^[A-Za-z]{1,3}$')
PATRON_SOLO_LETRAS = re.compile(r'^[A-Za-z]+$')

def limpiar_y_categorizar_dni_v3(df, columna_original, columna_salida=None, crear_motivo=True):
    if columna_salida is None: columna_salida = columna_original
    motivo_col = f"{columna_salida}_motivo" if crear_motivo else None

    def procesar_valor(v):
        if pd.isna(v): return ('NO BRINDO/NO VISIBLE', 'nan')
        s = str(v).strip()
        if s == '': return ('NO BRINDO/NO VISIBLE', 'empty')
        s_lower = s.lower()
        if PATRON_NO_BRINDO_GENERICOS.search(s_lower): return ('NO BRINDO/NO VISIBLE', 'patron_no_brindo_genericos')
        if PATRON_NO_BRINDO_SIMBOLOS.match(s) or PATRON_LETRAS_CORTAS.match(s): return ('NO BRINDO/NO VISIBLE', 'simbolos_o_letras_cortas')
        if PATRON_SOLO_LETRAS.match(s) and len(set(s_lower)) <= 2: return ('NO BRINDO/NO VISIBLE', 'solo_letras_repetidas')
        if PATRON_EXTRANJERO.search(s_lower): return ('CONTACTO EXTRANJERO', 'patron_extranjero')
        digits = re.sub(r'\D', '', s)
        if 6 <= len(digits) <= 10: return (int(digits), 'dni_valido')
        if len(digits) < 6 or re.search(r'[A-Za-z]', s_lower): return ('NO BRINDO/NO VISIBLE', 'texto_o_corto')
        return ('NO BRINDO/NO VISIBLE', 'resto_no_brindo')

    print(f"⚙️ Procesando DNI: {columna_original}...")
    resultados = df[columna_original].apply(procesar_valor)
    df[columna_salida] = resultados.apply(lambda x: x[0])
    if crear_motivo: df[motivo_col] = resultados.apply(lambda x: x[1])
    return df

# --- CATEGORIZACIÓN ---
CATEGORIAS_BRINDA_DATOS = ["traslado efectivo a cis", "acepta cis pero no hay vacante", "se activa protocolo de salud mental", "derivacion a same", "traslado/acompanamiento a otros efectores", "mendicidad (menores de edad)"]
CATEGORIAS_NO_BRINDA_DATOS = ["se realiza entrevista", "rechaza entrevista y se retira del lugar", "imposibilidad de abordaje por consumo", "rechaza entrevista y se queda en el lugar", "derivacion a espacio publico", "no se encuentra en situacion de calle"]
CATEGORIAS_NO_CONTACTA = ["no se contacta y se observan pertenencias", "no se contacta y no se observan pertenencias", "sin cubrir", "desestimado (cartas 911 u otras areas)"]
CATEGORIAS_TODAS = CATEGORIAS_BRINDA_DATOS + CATEGORIAS_NO_BRINDA_DATOS + CATEGORIAS_NO_CONTACTA

PATRONES_EXACTOS = {
    "17 dipa derivacion a cis": "traslado efectivo a cis",
    "01 positivo traslado a cis hogar 08 positivo derivacion a sas cud cp identidad etc": "traslado efectivo a cis",
    '10 se contacta pero rechaza pp por desconocimiento voluntad etc':'se realiza entrevisa',
    '21 asesoramiento sobre programas': 'se realiza entrevista',
    '16 dipa entrega de insumos servicios 21 asesoramiento sobre programas' : 'traslado/acompanamiento a otros efectores',
    '16 dipa entrega de insumos servicios 7 positivo entrega de insumos' : 'traslado/acompanamiento a otros efectores',
    '21 asesoramiento sobre programas 16 dipa entrega de insumos servicios':'traslado/acompanamiento a otros efectores',
    '7 positivo entrega de insumos 16 dipa entrega de insumos servicios':'traslado/acompanamiento a otros efectores',
    '7 positivo entrega de insumos 21 asesoramiento sobre programas':'traslado/acompanamiento a otros efectores',
    '21 asesoramiento sobre programas 7 positivo entrega de insumos':'traslado/acompanamiento a otros efectores',
    '16 dipa entrega de insumos servicios 21 asesoramiento sobre programas 7 positivo entrega de insumos':'traslado/acompanamiento a otros efectores',
    '11 se contacta pero rechaza pp por disconformidad egresado' : 'rechaza entrevista y se retira del lugar',
    '10 se contacta pero rechaza pp por desconocimiento voluntad etc 11 se contacta pero rechaza pp por disconformidad egresado':'rechaza entrevista y se retira del lugar',
    '9 se contacta pero rechaza entrevista 21 asesoramiento sobre programas':'rechaza entrevista y se retira del lugar',
    '9 se contacta pero rechaza entrevista 24 persona abandona el lugar por intervencion ep policia':'rechaza entrevista y se retira del lugar',
    '21 asesoramiento sobre programas 10 se contacta pero rechaza pp por desconocimiento voluntad etc':'rechaza entrevista y se retira del lugar',
    '10 se contacta pero rechaza pp por desconocimiento voluntad etc 21 asesoramiento sobre programas':'rechaza entrevista y se retira del lugar',
    '21 asesoramiento sobre programas 9 se contacta pero rechaza entrevista':'rechaza entrevista y se retira del lugar',
    '10 se contacta pero rechaza pp por desconocimiento voluntad etc 9 se contacta pero rechaza entrevista':'rechaza entrevista y se retira del lugar',
    'asesoramiento sobre programas rechan entrevista se quedan en el lugar':'rechaza entrevista y se queda en el lugar',
    '10 se contacta pero rechaza pp por desconocimiento voluntad etc': 'rechaza entrevista y se retira del lugar',
}

PATRONES_PERSONALIZADOS = {
    "derivacion a cis": "traslado efectivo a cis",
    "traslado a cis": "traslado efectivo a cis",
    "acepta cis": "acepta cis pero no hay vacante",
    "protocolo de salud mental": "se activa protocolo de salud mental",
    " same": "derivacion a same",
    "otros efectores": "traslado/acompanamiento a otros efectores",
    "mendicidad": "mendicidad (menores de edad)",
    "se realiza entrevista": "se realiza entrevista",
    "rechaza entrevista y se retira": "rechaza entrevista y se retira del lugar",
    "rechaza entrevista y se queda": "rechaza entrevista y se queda en el lugar",
    "imposibilidad de abordaje": "imposibilidad de abordaje por consumo",
    "espacio publico": "derivacion a espacio publico",
    "no se encuentra en situacion de calle": "no se encuentra en situacion de calle",
    "sin cubrir": "sin cubrir",
    "desestimado": "desestimado (cartas 911 u otras areas)"
}

def mapear_categoria_con_reglas(texto):
    if texto in PATRONES_EXACTOS: return PATRONES_EXACTOS[texto]
    for patron, categoria in PATRONES_PERSONALIZADOS.items():
        if patron in texto: return categoria
    
    # Fuzzy match
    mejor_match, score, _ = process.extractOne(texto, CATEGORIAS_TODAS, scorer=fuzz.WRatio)
    return mejor_match if score >= 80 else "sin_match"

def obtener_niveles(cat):
    if cat in CATEGORIAS_BRINDA_DATOS: return "Contacta", "Brinda datos"
    elif cat in CATEGORIAS_NO_BRINDA_DATOS: return "Contacta", "No brinda datos"
    elif cat in CATEGORIAS_NO_CONTACTA: return "No se contacta", ""
    else: return "Derivaciones/seguimientos", ""

# ==========================================
# LÓGICA PRINCIPAL DEL PROCESO
# ==========================================

def procesar_datos(excel_content_bytes, folder_id, watermark=None):
    """
    Procesa datos con soporte para carga incremental.
    
    Args:
        excel_content_bytes: Bytes del archivo Excel descargado
        folder_id: ID de la carpeta de Drive donde guardar
        watermark: Fecha máxima del histórico (para filtrado incremental)
    """
    service = get_drive_service()
    
    # ---------------------------------------------------------
    # FASE 1: ACTUALIZACIÓN DEL CRUDO (INCREMENTAL)
    # ---------------------------------------------------------
    print("🚀 Iniciando Fase 1: Actualización del Crudo (Modo Incremental)...")
    
    df_nuevo = pd.read_excel(io.BytesIO(excel_content_bytes), skiprows=1)
    nombre_crudo = "2025_historico_v2.parquet"
    df_hist = download_parquet_as_df(service, nombre_crudo, folder_id)

    # Normalización de Fechas
    col_fecha = 'Fecha Inicio'
    for df in [df_hist, df_nuevo]:
        if not df.empty:
            for col in [col_fecha, 'Fecha Fin', 'Recurso Fecha Liberado', 'Recurso Fecha asignacion', 'Recurso Arribo']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

    # Normalización Lat/Lon
    for col in ['Latitud', 'Longitud']:
        df_nuevo[col] = df_nuevo[col].astype(str).str.replace(',', '.', regex=False).astype(float)

    # CAMBIO CRÍTICO: Filtrado incremental basado en watermark
    if watermark is not None:
        print(f"📅 Aplicando filtro incremental: Fecha Inicio > {watermark}")
        df_filtrado_nuevo = df_nuevo[df_nuevo[col_fecha] > watermark]
        print(f"   Registros en Excel: {len(df_nuevo):,}")
        print(f"   Registros nuevos (post-watermark): {len(df_filtrado_nuevo):,}")
        
        if len(df_filtrado_nuevo) == 0:
            print("⚠️ No hay registros nuevos para procesar.")
            print("   El Excel no contiene datos más recientes que el histórico.")
            print("   Finalizando proceso sin cambios.")
            return None
    elif not df_hist.empty:
        # Fallback: usar MAX del histórico como en la lógica original
        fecha_corte = df_hist[col_fecha].max()
        print(f"📅 Watermark no recibido. Usando MAX del histórico: {fecha_corte}")
        df_filtrado_nuevo = df_nuevo[df_nuevo[col_fecha] > fecha_corte]
    else:
        df_filtrado_nuevo = df_nuevo
        print("📅 No hay histórico previo. Se procesará todo el Excel (primera carga).")

    # Concatenar y guardar
    if not df_filtrado_nuevo.empty:
        df_actualizado = pd.concat([df_hist, df_filtrado_nuevo], ignore_index=True)
        upload_df_as_parquet(service, df_actualizado, nombre_crudo, folder_id)
        print(f"✅ Se agregaron {len(df_filtrado_nuevo):,} registros nuevos al crudo.")
        print(f"   Total en histórico crudo: {len(df_actualizado):,} registros")
    else:
        print("⚠️ No hay registros nuevos para agregar. Usando histórico existente.")
        df_actualizado = df_hist

    # Limpieza de memoria
    del df_hist, df_nuevo, df_filtrado_nuevo
    gc.collect()

    # ---------------------------------------------------------
    # FASE 2: ENRIQUECIMIENTO GEOGRÁFICO (COMUNAS)
    # ---------------------------------------------------------
    print("🌍 Iniciando Fase 2: Spatial Join con Comunas...")
    
    # --- PASO 1: CLASIFICACIÓN DE ZONAS ESPECIALES (KMZ) ---
    # Inicializar comuna_calculada como None
    df_actualizado['comuna_calculada'] = None
    
    # Habilitar soporte KML en fiona
    fiona.drvsupport.supported_drivers['KML'] = 'rw'
    fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'
    
    # Convertir DataFrame a GeoDataFrame (una sola vez)
    df_actualizado['geometry'] = df_actualizado.apply(lambda row: Point(row['Longitud'], row['Latitud']), axis=1)
    puntos_gdf = gpd.GeoDataFrame(df_actualizado, crs="EPSG:4326")
    
    # PASO 1: Palermo Norte (Comuna 14.5) - PRIMERO
    print("📍 PASO 1: Clasificando puntos dentro de Palermo Norte...")
    ruta_palermo_norte = os.path.join(os.path.dirname(__file__), 'assets', 'comunas', 'Palermo_Norte.kmz')
    
    if not os.path.exists(ruta_palermo_norte):
        raise FileNotFoundError(f"❌ No encuentro el archivo KMZ en: {ruta_palermo_norte}")
    
    with zipfile.ZipFile(ruta_palermo_norte, 'r') as kmz:
        kml_files = [f for f in kmz.namelist() if f.endswith('.kml')]
        if not kml_files:
            raise FileNotFoundError(f"❌ No se encontró archivo KML dentro del KMZ: {ruta_palermo_norte}")
        
        with kmz.open(kml_files[0]) as kml_file:
            gdf_palermo_norte = gpd.read_file(kml_file)
    
    # Asegurar mismo CRS
    if puntos_gdf.crs != gdf_palermo_norte.crs:
        gdf_palermo_norte = gdf_palermo_norte.to_crs(puntos_gdf.crs)
    
    # Spatial Join con Palermo Norte
    resultado_palermo = gpd.sjoin(puntos_gdf, gdf_palermo_norte[['geometry']], how="left", predicate="within")
    
    # Identificar puntos dentro de Palermo Norte
    mask_palermo = resultado_palermo['index_right'].notna()
    
    # Asignar 14.5 (código para Palermo Norte) a los puntos que caen dentro
    df_actualizado.loc[mask_palermo, 'comuna_calculada'] = 14.5
    
    print(f"✅ Puntos clasificados como Palermo Norte (14.5): {mask_palermo.sum()}")
    
    del resultado_palermo, gdf_palermo_norte
    gc.collect()
    
    # PASO 2: CLASIFICACIÓN DE COMUNAS (SHP) - SEGUNDO
    # IMPORTANTE: Solo clasificar puntos que AÚN NO tienen comuna asignada
    print("📍 PASO 2: Ejecutando cruce espacial con comunas para puntos sin clasificar...")
    
    # Ruta dinámica al shapefile (assets dentro del src)
    ruta_shp = os.path.join(os.path.dirname(__file__), 'assets', 'comunas', 'comunas.shp')
    
    if not os.path.exists(ruta_shp):
        raise FileNotFoundError(f"❌ No encuentro el shapefile en: {ruta_shp}")

    gdf_comunas = gpd.read_file(ruta_shp)
    
    # Asegurar mismo CRS
    if puntos_gdf.crs != gdf_comunas.crs:
        gdf_comunas = gdf_comunas.to_crs(puntos_gdf.crs)

    # CRÍTICO: Solo procesar puntos donde comuna_calculada es None
    # Esto preserva la clasificación de Palermo Norte (14.5)
    mask_sin_clasificar = df_actualizado['comuna_calculada'].isna()
    puntos_sin_clasificar_gdf = puntos_gdf[mask_sin_clasificar].copy()
    
    print(f"📊 Puntos sin clasificar que irán al SHP: {mask_sin_clasificar.sum()}")
    
    if len(puntos_sin_clasificar_gdf) > 0:
        resultado_sjoin = gpd.sjoin(puntos_sin_clasificar_gdf, gdf_comunas[['comuna', 'geometry']], how="left", predicate="within")
        
        # Asignar comunas SOLO a los puntos que no tenían clasificación
        df_actualizado.loc[mask_sin_clasificar, 'comuna_calculada'] = resultado_sjoin['comuna'].values
        
        del resultado_sjoin
    
    # Limpiar geometría
    df_actualizado = df_actualizado.drop(columns=['geometry'])
    
    # Verificar distribución final
    print(f"✅ Distribución final de comuna_calculada:")
    print(f"   - Palermo Norte (14.5): {(df_actualizado['comuna_calculada'] == 14.5).sum()}")
    print(f"   - Comunas regulares: {df_actualizado['comuna_calculada'].between(1, 15, inclusive='both').sum()}")
    
    # comuna_calculada queda como float (comunas 1.0-15.0, zona especial: 14.5)
    
    del puntos_gdf, puntos_sin_clasificar_gdf, gdf_comunas
    gc.collect()

    # ---------------------------------------------------------
    # FASE 3: LIMPIEZA Y CATEGORIZACIÓN (CLEAN)
    # ---------------------------------------------------------
    print("🧹 Iniciando Fase 3: Limpieza y Categorización...")
    
    # 1. Limpieza DNI
    df_actualizado = limpiar_y_categorizar_dni_v3(df_actualizado, 'Persona DNI', columna_salida='DNI_Categorizado')
    df_actualizado['DNI_Categorizado'] = df_actualizado['DNI_Categorizado'].astype(str)

    # 2. Limpieza Nombres
    df_actualizado['Persona Nombre'] = df_actualizado['Persona Nombre'].apply(limpiar_texto)
    df_actualizado['Persona Apellido'] = df_actualizado['Persona Apellido'].apply(limpiar_texto)

    # 3. Eliminar Agencias
    agencias_a_eliminar = ['DIPA I COMBATE', 'MAPA DE RIESGO - SEGUIMIENTO', 'MAPA DE REISGO - SEGUIMIENTO','DIPA II ZABALA', 'AREA OPERATIVA', 'SALUD MENTAL']
    df_actualizado = df_actualizado[~df_actualizado['Agencia'].isin(agencias_a_eliminar)]

    # 4. Categorización
    valores_vacios = ['', ' ', '-', 'N/A', '(Vacio)', 'SIN DATO', 'nan', 'NAN', None]
    df_actualizado['Cierre Supervisor'] = df_actualizado['Cierre Supervisor'].replace(valores_vacios, np.nan)
    df_actualizado['Resultado'] = df_actualizado['Resultado'].replace(valores_vacios, np.nan)
    
    df_actualizado['cierre_texto'] = np.where(pd.isna(df_actualizado['Cierre Supervisor']), df_actualizado['Resultado'], df_actualizado['Cierre Supervisor'])
    df_actualizado['texto_limpio'] = df_actualizado['cierre_texto'].apply(limpiar_texto_cierre)
    
    print("🧠 Aplicando reglas y Fuzzy Match...")
    df_actualizado['categoria_final'] = df_actualizado['texto_limpio'].apply(mapear_categoria_con_reglas)

    # 5. Niveles
    niveles = df_actualizado['categoria_final'].apply(lambda x: obtener_niveles(x))
    df_actualizado['contacto'] = niveles.apply(lambda x: x[0])
    df_actualizado['brinda_datos'] = niveles.apply(lambda x: x[1])

    # === INICIO BLOQUE EVOLUCIÓN DNI (Exact dashboardgenerator replication) ===
    print("🧠 Calculando evolución histórica de DNI (Python) - Lógica dashboardgenerator exacta...")
    
    # 1. Ordenar por fecha (cronológico)
    df_actualizado = df_actualizado.sort_values('Fecha Inicio').reset_index(drop=True)
    
    # 2. Crear columna de Semana (mismo formato que dashboardgenerator)
    df_actualizado['Semana'] = df_actualizado['Fecha Inicio'].dt.to_period("W-SUN").apply(lambda r: r.start_time)
    
    # 3. Definir anónimos (no se clasifican)
    anonimos = ['NO BRINDO/NO VISIBLE', 'NO BRINDO', 'NO VISIBLE', 'S/D']
    
    # 4. Drop duplicates por Semana + DNI SOLAMENTE (NO por comuna)
    print("🔄 Eliminando duplicados semanales (Semana + DNI)...")
    
    # Guardar anónimos aparte (no se deduplicean)
    mask_anonimos = df_actualizado['DNI_Categorizado'].isin(anonimos)
    df_anonimos = df_actualizado[mask_anonimos].copy()
    df_no_anonimos = df_actualizado[~mask_anonimos].copy()
    
    # Eliminar duplicados SOLO en no-anónimos
    df_sem = df_no_anonimos.drop_duplicates(
        subset=['Semana', 'DNI_Categorizado'], 
        keep='last'  # Mantener el ÚLTIMO registro de cada DNI por semana
    ).copy()
    
    registros_eliminados = len(df_no_anonimos) - len(df_sem)
    print(f"📊 Eliminados {registros_eliminados} registros duplicados (keep='last')")
    
    # 5. CLASIFICACIÓN ITERATIVA POR SEMANA (matching dashboardgenerator)
    print("🔄 Clasificando DNIs semana por semana...")
    
    semanas = sorted(df_sem['Semana'].unique())
    dni_last_comuna = {}  # Diccionario: DNI -> última comuna vista
    dni_seen = set()      # Set de todos los DNIs que hemos visto
    
    # Lista para almacenar resultados de clasificación
    clasificaciones = []
    
    for semana in semanas:
        rows_sem = df_sem[df_sem['Semana'] == semana]
        
        # Para cada registro de esta semana, clasificarlo
        for idx, row in rows_sem.iterrows():
            dni = row['DNI_Categorizado']
            comuna_actual = row['comuna_calculada']
            
            prior_comuna = dni_last_comuna.get(dni, None)
            
            # LÓGICA DE CLASIFICACIÓN (exacta de dashboardgenerator):
            if prior_comuna is None and dni not in dni_seen:
                # Nuevo: primera vez que vemos este DNI
                clasificacion = 'Nuevos'
            else:
                # Ya fue visto
                if prior_comuna is not None and prior_comuna == comuna_actual:
                    # Recurrente: su última comuna era esta misma
                    clasificacion = 'Recurrentes'
                else:
                    # Migratorio: viene de otra comuna (o caso borde)
                    clasificacion = 'Migratorios'
            
            clasificaciones.append((idx, clasificacion))
        
        # CRÍTICO: Actualizar historial para TODOS los DNIs de esta semana
        # (no solo los de la comuna que estamos analizando)
        for idx, row in rows_sem.iterrows():
            dni_last_comuna[row['DNI_Categorizado']] = row['comuna_calculada']
            dni_seen.add(row['DNI_Categorizado'])
    
    # 6. Aplicar clasificaciones al DataFrame
    for idx, clasificacion in clasificaciones:
        df_sem.at[idx, 'Tipo_Evolucion'] = clasificacion
    
    # 7. Anónimos siempre son "No clasificable"
    df_anonimos['Tipo_Evolucion'] = 'No clasificable'
    
    # 8. Recombinar anónimos y clasificados
    df_actualizado = pd.concat([df_sem, df_anonimos], ignore_index=True)
    df_actualizado = df_actualizado.sort_values('Fecha Inicio').reset_index(drop=True)
    
    # Limpieza de columnas temporales
    df_actualizado.drop(columns=['Semana'], inplace=True, errors='ignore')
    
    print(f"✅ Clasificación completada - Lógica EXACTA de dashboardgenerator replicada")
    # === FIN BLOQUE EVOLUCIÓN DNI ===

    # ---------------------------------------------------------
    # GUARDADO FINAL (DRIVE Y NEON INCREMENTAL)
    # ---------------------------------------------------------
    nombre_limpio = "2025_historico_limpio.parquet"
    
    # 1. Subida a Drive (REEMPLAZA TODO EL ARCHIVO - Source of Truth)
    # Drive mantiene el histórico completo como fuente de verdad
    upload_df_as_parquet(service, df_actualizado, nombre_limpio, folder_id)
    
    # 2. Subida INCREMENTAL a Neon PostgreSQL
    # CAMBIO CRÍTICO: Ahora usa 'append' en lugar de 'replace'
    # Solo cargamos los registros nuevos procesados en esta ejecución
    TABLE_NAME = 'historico_limpio'
    
    if watermark is not None and not df_filtrado_nuevo.empty:
        # Carga incremental: solo subir los registros nuevos procesados
        print(f"\n📤 Preparando carga incremental a Neon...")
        
        # Necesitamos aplicar las mismas transformaciones al subset de datos nuevos
        # para mantener consistencia con Drive
        # Por simplicidad, cargaremos TODO el df_actualizado con REPLACE por ahora
        # TODO futuro: optimizar para cargar solo df_filtrado_nuevo con APPEND
        print(f"⚠️  NOTA: Actualmente se hace REPLACE completo por compatibilidad")
        print(f"   Esto garantiza consistencia total entre Drive y Neon")
        upload_to_neon_incremental(df_actualizado, TABLE_NAME, if_exists='replace')
    else:
        # Primera carga o recarga completa
        print(f"\n📤 Carga completa a Neon (primera vez o sin watermark)...")
        upload_to_neon_incremental(df_actualizado, TABLE_NAME, if_exists='replace')
    
    print(f"\n🎉 Proceso Terminado. Limpio actualizado al día {df_actualizado[col_fecha].max()}")
    print(f"   Total de registros procesados: {len(df_actualizado):,}")
    
    return df_actualizado