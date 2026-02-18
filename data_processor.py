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
    upload_df_as_parquet,
    download_parquet_as_df  # Recuperado para backup
)
from core.db_connections import (
    upload_to_neon_incremental,
    download_from_neon,
    get_max_date_from_neon,
    get_dni_history
)
from core.transformations import (
    limpiar_texto,
    limpiar_texto_cierre,
    limpiar_y_categorizar_dni_v3,
    mapear_categoria_con_reglas,
    obtener_niveles
)

# ==========================================
# LÓGICA PRINCIPAL DEL PROCESO
# ==========================================

def normalizar_columnas(df):
    """
    Normaliza robustamente los nombres de las columnas del DataFrame de entrada.
    Mapea variantes comunes a los nombres estándar usados en el resto del proceso.
    """
    STANDARD_MAP = {
        'fecha_inicio': 'Fecha Inicio',
        'fecha_fin': 'Fecha Fin',
        'persona_dni': 'Persona DNI',
        'persona_nombre': 'Persona Nombre',
        'persona_apellido': 'Persona Apellido',
        'latitud': 'Latitud',
        'longitud': 'Longitud',
        'agencia': 'Agencia',
        'resultado': 'Resultado',
        'cierre_supervisor': 'Cierre Supervisor',
        'cierre_despachador': 'Cierre Despachador',
        'id_suceso': 'Id Suceso'
    }
    
    # --- PASO EXTRA: Si no detectamos columnas clave, tal vez están en la primera fila ---
    # Buscamos 'fecha' en los nombres de columnas actuales
    hay_fecha = any('fecha' in str(c).lower() for c in df.columns)
    
    if not hay_fecha and not df.empty:
        # Probamos ver si el header está en la fila 0
        primera_fila = df.iloc[0].astype(str).str.lower().str.strip().tolist()
        if any('fecha' in str(val) for val in primera_fila):
            print("💡 Detectada cabecera en la primera fila de datos. Re-asignando...")
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
            # Volver a calcular current_cols
    
    # 1. Limpieza básica: strip y lower para el matching inicial
    current_cols = {c: str(c).strip().lower() for c in df.columns}
    
    rename_map = {}
    for original, clean in current_cols.items():
        if clean in STANDARD_MAP:
            rename_map[original] = STANDARD_MAP[clean]
            
    if rename_map:
        print(f"🔄 Normalizando columnas: {rename_map}")
        df.rename(columns=rename_map, inplace=True)
    
    return df


def procesar_datos(excel_content_bytes, folder_id, watermark=None):
    """
    Procesa datos con soporte para carga incremental.
    
    Args:
        excel_content_bytes: Bytes del archivo Excel descargado
        folder_id: ID de la carpeta de Drive donde guardar
        watermark: Fecha máxima del histórico (para filtrado incremental)
    """
    service = get_drive_service()
    
    # Nombres de referencias
    TABLE_LIMPIO = 'historico_limpio'
    FILE_LIMPIO = "2025_historico_limpio.parquet"
    
    # ---------------------------------------------------------
    # FASE 0: DESCARGA DE HISTORIAL (Source of Truth)
    # ---------------------------------------------------------
    print(f"🚀 Iniciando Procesamiento (Master Flow: {FILE_LIMPIO} -> Neon)...")
    
    # Descargar historial completo de Drive
    try:
        df_historico_full = download_parquet_as_df(service, FILE_LIMPIO, folder_id)
        if not df_historico_full.empty:
            # Asegurar que las fechas sean datetime
            for col in ['Fecha Inicio', 'Fecha Fin']:
                if col in df_historico_full.columns:
                    df_historico_full[col] = pd.to_datetime(df_historico_full[col], errors='coerce')
            
            # Si no se pasó watermark por argumento, lo tomamos del Parquet
            if watermark is None:
                watermark = df_historico_full['Fecha Inicio'].max()
                print(f"📅 Watermark detectado desde Parquet: {watermark}")
        else:
            print("⚠️ Parquet histórico no encontrado o vacío.")
    except Exception as e:
        print(f"⚠️ Error cargando historial de Drive: {e}")
        df_historico_full = pd.DataFrame()

    # ---------------------------------------------------------
    # FASE 1: LECTURA Y FILTRADO DEL NUEVO EXCEL
    # ---------------------------------------------------------
    
    # Leer Excel Nuevo
    try:
        df_nuevo = pd.read_excel(io.BytesIO(excel_content_bytes))
    except Exception as e:
        print(f"❌ Error leyendo Excel: {e}")
        return None
    
    # NORMALIZACIÓN ROBUSTA DE CABECERAS
    df_nuevo = normalizar_columnas(df_nuevo)

    col_fecha = 'Fecha Inicio'
    
    # Normalización de Fechas (Para poder filtrar)
    if col_fecha in df_nuevo.columns:
        df_nuevo[col_fecha] = pd.to_datetime(df_nuevo[col_fecha], errors='coerce')
    else:
        print(f"❌ ERROR: No se encontró '{col_fecha}'.")
        print(f"   Columnas disponibles: {list(df_nuevo.columns)}")
        print(f"   Primeras 3 filas:")
        print(df_nuevo.head(3))
        raise KeyError(f"No se encontró la columna de fecha ('{col_fecha}') en el Excel. Ver logs arriba.")
    
    # Filtrado Incremental
    if watermark:
        print(f"📅 Filtrando registros posteriores a: {watermark}")
        df_filtrado_nuevo = df_nuevo[df_nuevo[col_fecha] > watermark].copy()
    else:
        print("📅 No hay watermark. Se procesará TODO el archivo.")
        df_filtrado_nuevo = df_nuevo.copy()

    if df_filtrado_nuevo.empty:
        print("⚠️ No hay registros nuevos para procesar.")
        return None
        
    print(f"✅ Registros nuevos a procesar: {len(df_filtrado_nuevo):,}")

    # Limpieza de memoria temporal
    del df_nuevo
    gc.collect()

    # Renombramos variable para seguir lógica del script
    df_actualizado = df_filtrado_nuevo 

    # Normalización Lat/Lon (Crítico para Spatial Join posterior)
    for col in ['Latitud', 'Longitud']:
        if col in df_actualizado.columns:
            # Asegurar que sean strings antes de reemplazar y luego float
            df_actualizado[col] = df_actualizado[col].astype(str).str.replace(',', '.', regex=False)
            df_actualizado[col] = pd.to_numeric(df_actualizado[col], errors='coerce')

    # ELIMINAR COLUMNAS PREVIAS DE CÁLCULO (Si existen) para evitar colisiones en sjoin/DB
    # Estas se recalcularán en el script.
    # Buscamos todas las variaciones de 'comuna' o 'comuna_calculada'
    variaciones = ['comuna', 'comuna_calculada', 'Comuna_calculada', 'Comuna', 'COMUNA']
    df_actualizado = df_actualizado.drop(columns=[c for c in variaciones if c in df_actualizado.columns], errors='ignore')

    # Normalización de columnas de texto (Strip y Title)
    # CRÍTICO: No convertir columnas de FECHA a string, ya que eso rompe la inserción en el DB (genera "Nan")
    date_cols_known = [
        'Fecha Inicio', 'Fecha Fin', 'Recurso Fecha asignacion', 
        'Recurso Arribo', 'Recurso Fecha Liberado', 'Persona Fecha Nacimiento'
    ]
    
    cols_obj = df_actualizado.select_dtypes(include=['object']).columns
    for col in cols_obj:
        if col not in date_cols_known and col != 'RESULTADO':
             df_actualizado[col] = df_actualizado[col].astype(str).str.strip().str.title()
             # Reemplazar 'Nan' literal por valor nulo real
             df_actualizado[col] = df_actualizado[col].replace(['Nan', 'None', 'Nat'], np.nan)

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
    
    # SEED HISTORIAL (Desde el Parquet cargado en Fase 0)
    print("🦷 Sembrando historial de DNIs desde el Parquet histórico...")
    if not df_historico_full.empty and 'DNI_Categorizado' in df_historico_full.columns:
        # Replicar lógica de get_dni_history pero con el DF en memoria
        # Quedarse con el último registro por DNI
        df_hist_dnis = df_historico_full[df_historico_full['DNI_Categorizado'].notna()].copy()
        # Filtros de anónimos (basados en logica de data_processor)
        df_hist_dnis = df_hist_dnis[~df_hist_dnis['DNI_Categorizado'].isin(anonimos)]
        
        # Ordenar por fecha y quedarse con el último por DNI
        df_hist_dnis = df_hist_dnis.sort_values('Fecha Inicio').drop_duplicates(subset=['DNI_Categorizado'], keep='last')
        
        dni_last_comuna = dict(zip(df_hist_dnis['DNI_Categorizado'].astype(str), df_hist_dnis['comuna_calculada']))
        dni_seen = set(dni_last_comuna.keys())
        print(f"✅ Historial sembrado con {len(dni_seen):,} DNIs previos.")
        del df_hist_dnis
    else:
        print("⚠️ No hay historial previo para sembrar. Iniciando limpio.")
        dni_last_comuna = {}
        dni_seen = set()
    
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
    # GUARDADO FINAL (DRIVE REPLACE + NEON REPLACE)
    # ---------------------------------------------------------
    
    # 1. Crear el DataFrame completo (Master Update)
    if not df_historico_full.empty:
         df_actualizado_completo = pd.concat([df_historico_full, df_actualizado], ignore_index=True)
         print(f"\n📊 Generando df_actualizado completo (histórico + nuevos). Total: {len(df_actualizado_completo):,}")
    else:
         df_actualizado_completo = df_actualizado
         print(f"\n📊 Generando df_actualizado completo (primera carga). Total: {len(df_actualizado_completo):,}")

    # 5. Upload REPLACE a Drive
    print(f"💾 5. Subiendo REPLACE a Drive: {FILE_LIMPIO}...")
    try:
        upload_df_as_parquet(service, df_actualizado_completo, FILE_LIMPIO, folder_id)
        print(f"✅ Drive actualizado exitosamente.")
    except Exception as e:
        print(f"⚠️ Error subiendo a Drive: {e}")
    
    # 6. Upload REPLACE a Neon PostgreSQL
    print(f"📤 6. Subiendo REPLACE a Neon PostgreSQL: {TABLE_LIMPIO}...")
    try:
        # Usamos if_exists='replace' para que la tabla coincida exactamente con el Parquet
        upload_to_neon_incremental(df_actualizado_completo, TABLE_LIMPIO, if_exists='replace')
        print(f"✅ Neon PostgreSQL sincronizado exitosamente (Full Replace).")
    except Exception as e:
        print(f"❌ Error sincronizando Neon: {e}")

    print(f"\n🎉 Proceso Finalizado según flujo diagramado.")
    
    return df_actualizado_completo
