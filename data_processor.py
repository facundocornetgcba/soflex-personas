#!/usr/bin/env python3
"""
data_processor.py
================================================================
ETL incremental semanal.

Flujo:
  1. Lee el Excel nuevo y filtra solo filas > watermark
  2. Geo-enriquece (Palermo Norte KMZ + SHP comunas)
  3. Limpia y categoriza (DNI, nombres, categoras, niveles)
  4. Para clasificar Tipo_Evolucion correctamente, baja de Neon
     el historial previo (solo DNI + semana + comuna) y construye
     el estado inicial (ultima_comuna, dni_seen) desde ah.
     As un DNI que ya fue visto semanas anteriores NO sale "Nuevo".
  5. Append a Neon con COPY (ultrarrpido)
  6. Append al parquet de Drive (descarga solo para concatenar y volver
     a subir - Neon es la fuente de verdad, Drive es backup)
================================================================
"""

import gc
import io
import os
import zipfile
import sys

if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

from sqlalchemy import text as sa_text

from core.db_connections import get_neon_engine, get_max_date_from_neon
from core.drive_manager import (
    download_parquet_as_df,
    get_drive_service,
    upload_df_as_parquet,
)
from core.transformations import (
    limpiar_texto,
    limpiar_texto_cierre,
    limpiar_y_categorizar_dni_v3,
    mapear_categoria_con_reglas,
    obtener_nivel_contacto,
    obtener_niveles,
)

#  Constantes 

TABLE_NEON   = "historico_limpio"
FILE_PARQUET = "2025_historico_limpio.parquet"
FILE_RAW     = "2025_historico_v2.parquet"

ASSETS_DIR    = os.path.join(os.path.dirname(__file__), "assets", "comunas")
SHP_COMUNAS   = os.path.join(ASSETS_DIR, "comunas.shp")
KMZ_PALERMO_N = os.path.join(ASSETS_DIR, "Palermo_Norte.kmz")

AGENCIAS_EXCLUIR = {
    "DIPA I COMBATE", "MAPA DE RIESGO - SEGUIMIENTO",
    "MAPA DE REISGO - SEGUIMIENTO", "DIPA II ZABALA",
    "AREA OPERATIVA", "SALUD MENTAL",
}
VALORES_VACIOS = ["", " ", "-", "N/A", "(Vacio)", "SIN DATO", "nan", "NAN", None]

DNI_INVALIDOS = {
    "NO BRINDO/NO VISIBLE", "NO BRINDO", "NO VISIBLE", "S/D", "X",
    "NAN", "nan", "NaN", "", " ", "NONE", "None",
    "0","1","2","3","4","5","6","7","8","9",
    "123","1234","12345","11111111","00000000","111111","222222","333333", "-", "/"
}


#  Helpers 

def normalizar_comuna(valor) -> float:
    """Convierte cualquier representacin de comuna a float cannico."""
    if pd.isna(valor):
        return np.nan
    try:
        return float(str(valor).strip().replace(",", "."))
    except (ValueError, TypeError):
        return np.nan


def es_dni_valido(dni) -> bool:
    if pd.isna(dni):
        return False
    s = str(dni).strip()
    if s in DNI_INVALIDOS:
        return False
    if len(s) < 6:
        return False
    if len(set(s)) == 1:
        return False
    return s.replace(".", "").replace("-", "").isdigit()


def append_neon_copy(engine, df: pd.DataFrame, table_name: str, chunk_size: int = 50_000) -> None:
    """
    Append ultrarrpido con COPY FROM STDIN chunked.
    Commit por chunk para no acumular WAL en Neon free tier.
    Crea la tabla automticamente si no existe.
    """
    DTYPE_MAP = {
        "int64": "BIGINT", "int32": "INTEGER",
        "float64": "DOUBLE PRECISION", "float32": "REAL",
        "bool": "BOOLEAN", "object": "TEXT",
        "datetime64[ns]": "TIMESTAMP",
    }
    raw = engine.raw_connection()
    cur = raw.cursor()
    try:
        # Crear tabla si no existe
        cur.execute(f"SELECT to_regclass('{table_name}')")
        if not cur.fetchone()[0]:
            col_defs = ", ".join(
                f'"{c}" {DTYPE_MAP.get(str(t), "TEXT")}'
                for c, t in zip(df.columns, df.dtypes)
            )
            cur.execute(f"CREATE TABLE {table_name} ({col_defs});")
            raw.commit()
            print(f"   ⚡ Tabla '{table_name}' creada.")

        # --- NORMALIZACIN DE COLUMNAS PARA NEON ---
        # El Excel puede cambiar maysculas o espacios. Mapeamos al esquema real de Neon.
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            ORDER BY ordinal_position
        """)
        db_cols = [r[0] for r in cur.fetchall()]
        
        if db_cols:
            # Mapeo insensible a case, espacios Y guiones bajos
            df_cols_map = {c.lower().replace(" ", "").replace("_", ""): c for c in df.columns}
            rename_map = {}
            for db_c in db_cols:
                lookup = db_c.lower().replace(" ", "").replace("_", "")
                if lookup in df_cols_map:
                    rename_map[df_cols_map[lookup]] = db_c
            
            df = df.rename(columns=rename_map)
            
            # Asegurar que todas las columnas de la DB existen en el DF
            for db_c in db_cols:
                if db_c not in df.columns:
                    df[db_c] = np.nan
            
            # Reordenar y filtrar solo las que estn en la DB
            df = df[db_cols].copy()

        cols     = ", ".join(f'"{c}"' for c in df.columns)
        total    = len(df)
        n_chunks = (total // chunk_size) + (1 if total % chunk_size else 0)

        print(f"   ⚡ Append {total:,} filas en {n_chunks} chunks...")

        for i in range(n_chunks):
            chunk = df.iloc[i * chunk_size : (i + 1) * chunk_size]
            buf = io.StringIO()
            chunk.to_csv(buf, index=False, header=False,
                         na_rep="\\N", date_format="%Y-%m-%d %H:%M:%S")
            buf.seek(0)
            cur.copy_expert(
                f"COPY {table_name} ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                buf,
            )
            raw.commit()
            print(f"      [{i+1}/{n_chunks}] {min((i+1)*chunk_size, total):,}/{total:,}")

        print(f"   ✅ Append OK: {total:,} filas → Neon '{table_name}'")

    except Exception as exc:
        raw.rollback()
        raise RuntimeError(f"COPY append fall: {exc}") from exc
    finally:
        cur.close()
        raw.close()


#  Fase 1: Geo 

def calcular_comunas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asigna comuna_calculada (float) usando spatial join.
    Prioridad: Palermo Norte (14.5) > SHP comunas (1-15) > NaN
    FIX: normaliza coma->punto ANTES del join.
    """
    print("🌍 Geo: calculando comunas...")

    fiona.drvsupport.supported_drivers["KML"]    = "rw"
    fiona.drvsupport.supported_drivers["LIBKML"] = "rw"

    # Normalizar coordenadas (Estandarizar nombres primero)
    cols_map = {c.lower().replace(" ", ""): c for c in df.columns}
    for canonical in ("Latitud", "Longitud"):
        lookup = canonical.lower()
        if lookup in cols_map and cols_map[lookup] != canonical:
            df = df.rename(columns={cols_map[lookup]: canonical})

    for col in ("Latitud", "Longitud"):
        if col in df.columns:
            df[col] = (df[col].astype(str).str.strip()
                               .str.replace(",", ".", regex=False))
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["comuna_calculada"] = np.nan
    mask_geo = df["Latitud"].notna() & df["Longitud"].notna()

    if not mask_geo.any():
        print("   [WARN]  Sin coordenadas vlidas.")
        return df

    puntos = gpd.GeoDataFrame(
        df[mask_geo].copy(),
        geometry=gpd.points_from_xy(
            df.loc[mask_geo, "Longitud"], df.loc[mask_geo, "Latitud"]
        ),
        crs="EPSG:4326",
    )

    # Paso 1: Palermo Norte
    print("   [POINT] Palermo Norte (KMZ)...")
    with zipfile.ZipFile(KMZ_PALERMO_N) as kmz:
        kml_name = next(f for f in kmz.namelist() if f.endswith(".kml"))
        with kmz.open(kml_name) as kml:
            gdf_pn = gpd.read_file(kml)
    if puntos.crs != gdf_pn.crs:
        gdf_pn = gdf_pn.to_crs(puntos.crs)

    join_pn  = gpd.sjoin(puntos, gdf_pn[["geometry"]], how="left", predicate="within")
    mask_pn  = join_pn["index_right"].notna().values
    idx_geo  = df[mask_geo].index
    df.loc[idx_geo[mask_pn], "comuna_calculada"] = 14.5
    print(f"      Palermo Norte: {mask_pn.sum():,}")
    del join_pn, gdf_pn; gc.collect()

    # Paso 2: SHP comunas (solo los sin clasificar)
    print("   [POINT] SHP comunas...")
    gdf_com = gpd.read_file(SHP_COMUNAS)
    if puntos.crs != gdf_com.crs:
        gdf_com = gdf_com.to_crs(puntos.crs)

    mask_sin = mask_geo & df["comuna_calculada"].isna()
    if mask_sin.any():
        df_sin = df[mask_sin].copy()
        puntos_sin = gpd.GeoDataFrame(
            df_sin,
            geometry=gpd.points_from_xy(df_sin["Longitud"], df_sin["Latitud"]),
            crs="EPSG:4326",
        )
        join_com = gpd.sjoin(
            puntos_sin, gdf_com[["comuna", "geometry"]], how="left", predicate="within"
        )
        # Deduplicar ndice (puntos en borde de dos polgonos)
        join_com = join_com[~join_com.index.duplicated(keep="first")]
        df.loc[mask_sin, "comuna_calculada"] = join_com["comuna"].values

    del gdf_com, puntos; gc.collect()

    # Normalizar a float cannico
    df["comuna_calculada"] = df["comuna_calculada"].apply(normalizar_comuna)

    print(f"   [OK] Palermo Norte (14.5): {(df['comuna_calculada'] == 14.5).sum():,}")
    print(f"   [OK] Comunas 1-15:         {df['comuna_calculada'].between(1,15).sum():,}")
    print(f"   [WARN]  Sin comuna:           {df['comuna_calculada'].isna().sum():,}")
    return df


#  Fase 2: Limpieza 

def limpiar_y_categorizar(df: pd.DataFrame) -> pd.DataFrame:
    """DNI, nombres, agencias, categoras, niveles."""
    print("[CLEAN] Limpieza y categorizacin...")

    # DNI
    df = limpiar_y_categorizar_dni_v3(df, "Persona DNI", columna_salida="DNI_categorizado")
    df["DNI_categorizado"] = df["DNI_categorizado"].astype(str).str.strip()

    # Nombres
    df["Persona Nombre"]   = df["Persona Nombre"].apply(limpiar_texto)
    df["Persona Apellido"] = df["Persona Apellido"].apply(limpiar_texto)

    # Agencias (búsqueda case-insensitive: el Excel trae "Agencia" con mayúscula)
    cols_lower_agencia = {c.lower(): c for c in df.columns}
    col_agencia = cols_lower_agencia.get("agencia")
    if col_agencia:
        df = df[~df[col_agencia].isin(AGENCIAS_EXCLUIR)].copy()

    # --- Normalizacion robusta de columnas fuente (case-insensitive) ---
    # El Excel puede traer 'Resultado' en vez de 'resultado', 'Cierre Supervisor', etc.
    # Buscamos case-insensitive y renombramos al nombre canonico si difieren.
    cols_lower = {c.lower().replace(" ", "_"): c for c in df.columns}
    for col_canonico in ("cierre_supervisor", "resultado"):
        col_real = cols_lower.get(col_canonico)
        if col_real and col_real != col_canonico:
            df.rename(columns={col_real: col_canonico}, inplace=True)
            print(f"   [FIX] Columna '{col_real}' renombrada a '{col_canonico}'")

    # Categorizacin de cierre
    for col in ("cierre_supervisor", "resultado"):
        if col in df.columns:
            df[col] = df[col].replace(VALORES_VACIOS, np.nan)

    # Determinar la columna de texto disponible
    tiene_supervisor = "cierre_supervisor" in df.columns
    tiene_resultado  = "resultado" in df.columns

    if tiene_supervisor and tiene_resultado:
        # Usar cierre_supervisor si tiene valor; si no, usar resultado.
        # Convertir a str para evitar problemas con StringDtype <NA> en np.where.
        sup_str = df["cierre_supervisor"].astype(object)
        res_str = df["resultado"].astype(object)
        mask_sup_nulo = sup_str.isna()
        df["cierre_texto"] = np.where(mask_sup_nulo, res_str, sup_str)
    elif tiene_supervisor:
        print("   [WARN] Solo 'cierre_supervisor' disponible (sin 'resultado')")
        df["cierre_texto"] = df["cierre_supervisor"].astype(object)
    elif tiene_resultado:
        print("   [WARN] Solo 'resultado' disponible (sin 'cierre_supervisor')")
        df["cierre_texto"] = df["resultado"].astype(object)
    else:
        print("   [ERROR] No se encontraron columnas 'cierre_supervisor' ni 'resultado'. "
              "categoria_final quedara vacia.")
        print(f"   Columnas disponibles: {list(df.columns)}")
        return df

    df["texto_limpio"]    = df["cierre_texto"].apply(limpiar_texto_cierre)
    df["categoria_final"] = df["texto_limpio"].apply(mapear_categoria_con_reglas)
    df["nivel_contacto"]  = df["categoria_final"].apply(obtener_nivel_contacto)
    niveles               = df["categoria_final"].apply(obtener_niveles)
    df["contacto"]        = niveles.apply(lambda x: x[0])
    df["brinda_datos"]    = niveles.apply(lambda x: x[1])

    sin_match = (df["categoria_final"] == "sin_match").sum()
    print(f"   [OK] categoria_final calculada. sin_match: {sin_match:,} / {len(df):,}")

    return df


#  Fase 3: Tipo_Evolucion (incremental) 

def _build_estado_historico(engine) -> tuple[dict, set]:
    """
    Obtiene el ultimo estado de cada DNI desde Neon usando SQL agregado.
    En lugar de bajar toda la tabla, trae solo 1 fila por DNI (la mas reciente).
    Esto reduce 10x el volumen transferido y elimina el timeout en Neon free tier.

    Retorna:
        ultima_comuna: dict  dni_str -> float (ultima comuna conocida)
        dni_seen:      set   de todos los DNIs que ya estuvieron en Neon
    """
    import time

    print("   📡 Descargando ultimo estado por DNI desde Neon...")

    # Query agregada: 1 fila por DNI con su ultima comuna.
    # DISTINCT ON es nativo de Postgres y muy eficiente.
    q = f"""
        SELECT DISTINCT ON ("DNI_categorizado")
               "DNI_categorizado",
               "comuna_calculada"
        FROM   {TABLE_NEON}
        WHERE  "DNI_categorizado" IS NOT NULL
        ORDER  BY "DNI_categorizado", "Fecha Inicio" DESC
    """

    MAX_INTENTOS = 3
    df_hist = None

    for intento in range(1, MAX_INTENTOS + 1):
        engine_fresco = get_neon_engine()
        try:
            with engine_fresco.connect() as conn:
                df_hist = pd.read_sql(q, conn)
            engine_fresco.dispose()
            break
        except Exception as exc:
            engine_fresco.dispose()
            if intento < MAX_INTENTOS:
                espera = 5 * intento
                print(f"   ⚠️  Intento {intento}/{MAX_INTENTOS} fallido: {exc}")
                print(f"      Reintentando en {espera}s...")
                time.sleep(espera)
            else:
                print(f"   [ERROR] No se pudo cargar historial tras {MAX_INTENTOS} intentos: {exc}")
                print("      ADVERTENCIA: clasificando sin historial - DNIs existentes "
                      "saldran como Nuevos. Verifica la conexion a Neon.")
                return {}, set()

    if df_hist is None or df_hist.empty:
        print("   ⚠️  Historial vacio en Neon.")
        return {}, set()

    df_hist["comuna_calculada"] = df_hist["comuna_calculada"].apply(normalizar_comuna)

    ultima_comuna = dict(zip(df_hist["DNI_categorizado"].astype(str),
                             df_hist["comuna_calculada"]))
    dni_seen      = set(ultima_comuna.keys())

    print(f"   ✅ Estado historico: {len(dni_seen):,} DNIs unicos cargados")
    return ultima_comuna, dni_seen


def _calcular_apariciones_incremental(
    df_nuevo: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calcula la columna 'apariciones' para el lote nuevo de forma incremental.

    Lgica:
      - Para cada DNI vlido nico: cuenta cuntas veces apareci en el histrico
        de Neon + cuntas veces aparece en el lote nuevo.
      - Para DNIs invlidos (NO BRINDO, CONTACTO EXTRANJERO, etc.): apariciones = 0.

    La columna resultante representa el total ACUMULADO de intervenciones
    al mismo DNI desde el inicio del histrico, incluyendo este lote.
    """
    import time

    print("📊 Calculando apariciones acumuladas por DNI...")

    df = df_nuevo.copy()

    # DNI vlido: es dgitos, longitud >= 6 (misma lgica que es_dni_valido)
    mask_valido = df["DNI_categorizado"].apply(es_dni_valido)

    # Conteo del lote nuevo por DNI vlido
    conteo_nuevo = (
        df[mask_valido]
        .groupby("DNI_categorizado")
        .size()
        .to_dict()
    )

    # Conteo histrico desde Neon (solo DNIs del lote nuevo para no bajar todo)
    dnis_validos = list(conteo_nuevo.keys())
    conteo_historico = {}

    if dnis_validos:
        # Consultamos en chunks de 500 DNIs para no generar queries enormes
        CHUNK_DNI = 500
        frames_hist = []
        MAX_INTENTOS = 3

        for i in range(0, len(dnis_validos), CHUNK_DNI):
            chunk = dnis_validos[i : i + CHUNK_DNI]
            # ANY(:dnis) es seguro y no genera un IN gigante
            q = f"""
                SELECT "DNI_categorizado", COUNT(*) AS c
                FROM   {TABLE_NEON}
                WHERE  "DNI_categorizado" = ANY(:dnis)
                GROUP  BY "DNI_categorizado"
            """
            for intento in range(1, MAX_INTENTOS + 1):
                engine_fresco = get_neon_engine()
                try:
                    with engine_fresco.connect() as conn:
                        df_chunk = pd.read_sql(
                            sa_text(q), conn, params={"dnis": chunk}
                        )
                    engine_fresco.dispose()
                    frames_hist.append(df_chunk)
                    break
                except Exception as exc:
                    engine_fresco.dispose()
                    if intento < MAX_INTENTOS:
                        espera = 5 * intento
                        print(f"   [WARN] Intento {intento}/{MAX_INTENTOS} fallido: {exc}")
                        print(f"      Reintentando en {espera}s...")
                        time.sleep(espera)
                    else:
                        print(f"   [WARN] No se pudo obtener conteo histrico (chunk {i}): {exc}")
                        print("      apariciones chunk omitido (usando solo lote nuevo).")

        if frames_hist:
            df_hist_all = pd.concat(frames_hist, ignore_index=True)
            conteo_historico = dict(
                zip(df_hist_all["DNI_categorizado"].astype(str),
                    df_hist_all["c"].astype(int))
            )

    # Combinar: histrico + lote nuevo
    conteo_total = {}
    for dni, cnt_nuevo in conteo_nuevo.items():
        cnt_hist = conteo_historico.get(str(dni), 0)
        conteo_total[str(dni)] = int(cnt_hist) + int(cnt_nuevo)

    # Asignar al DataFrame
    def _get_apariciones(row):
        if not es_dni_valido(row["DNI_categorizado"]):
            return 0
        return conteo_total.get(str(row["DNI_categorizado"]), 1)

    df["apariciones"] = df.apply(_get_apariciones, axis=1)

    print(f"   ✅ apariciones calculadas: "
          f"max={df['apariciones'].max()}, "
          f"media={df['apariciones'].mean():.1f}, "
          f"DNIs con >1 visita={( df['apariciones'] > 1 ).sum():,}")
    return df


def clasificar_tipo_evolucion_incremental(
    df: pd.DataFrame,
    ultima_comuna: dict,
    dni_seen: set,
) -> pd.DataFrame:
    """
    Clasifica Tipo_Evolucion para los registros NUEVOS usando el
    estado histrico previo como punto de partida.

    Reglas (semana a semana):
      - DNI no visto nunca             -> Nuevos
      - Misma comuna que semana previa -> Recurrentes
      - Distinta comuna                -> Migratorios
      - DNI invlido/annimo           -> No clasificable
      - Sin coordenadas en algn lado  -> Recurrentes (conservador)
    """
    print("[AI] Clasificando Tipo_Evolucion (incremental)...")

    df = df.copy()
    df["Fecha Inicio"]     = pd.to_datetime(df["Fecha Inicio"], errors="coerce")
    df["comuna_calculada"] = df["comuna_calculada"].apply(normalizar_comuna)
    df["DNI_categorizado"] = df["DNI_categorizado"].astype(str).str.strip()

    mask_valido = df["DNI_categorizado"].apply(es_dni_valido)
    df_val   = df[mask_valido].copy()
    df_inval = df[~mask_valido].copy()

    df_val["__semana"] = (
        df_val["Fecha Inicio"]
        .dt.to_period("W-SUN")
        .apply(lambda p: p.start_time)
    )

    # Referencia de clasificacin: ltimo registro por (semana, DNI)
    df_ref = (
        df_val
        .sort_values("Fecha Inicio")
        .drop_duplicates(subset=["__semana", "DNI_categorizado"], keep="last")
        .copy()
    )

    semanas = sorted(df_ref["__semana"].unique())
    clf_ref = {}   # (semana, dni) -> label

    for semana in semanas:
        bloque = df_ref[df_ref["__semana"] == semana]

        for _, row in bloque.iterrows():
            dni    = row["DNI_categorizado"]
            comuna = row["comuna_calculada"]

            if dni not in dni_seen:
                label = "Nuevos"
            else:
                prev = ultima_comuna.get(dni, np.nan)
                if pd.isna(prev) or pd.isna(comuna):
                    label = "Recurrentes"   # conservador: sin coords no inflar Migratorios
                elif prev == comuna:
                    label = "Recurrentes"
                else:
                    label = "Migratorios"

            clf_ref[(semana, dni)] = label

        # Actualizar estado para la siguiente semana de este lote
        for _, row in bloque.iterrows():
            ultima_comuna[row["DNI_categorizado"]] = row["comuna_calculada"]
            dni_seen.add(row["DNI_categorizado"])

    # Propagar a TODOS los registros del mismo (semana, DNI)
    df_val["Tipo_Evolucion"] = df_val.apply(
        lambda r: clf_ref.get((r["__semana"], r["DNI_categorizado"]), "Nuevos"),
        axis=1,
    )

    # Si un DNI es "Nuevos" en una semana, dejar solo 1 fila como "Nuevos"
    # (la primera por Fecha Inicio) y el resto como "Nuevo repetido".
    mask_nuevos = df_val["Tipo_Evolucion"] == "Nuevos"
    if mask_nuevos.any():
        idx_keep = (
            df_val[mask_nuevos]
            .sort_values("Fecha Inicio")
            .groupby(["__semana", "DNI_categorizado"], sort=False)
            .head(1)
            .index
        )
        idx_all = df_val[mask_nuevos].index
        idx_rep = idx_all.difference(idx_keep)
        if len(idx_rep) > 0:
            df_val.loc[idx_rep, "Tipo_Evolucion"] = "Nuevo repetido"
    df_inval["Tipo_Evolucion"] = "No clasificable"

    df_out = pd.concat([df_val, df_inval], ignore_index=True)
    df_out  = df_out.sort_values("Fecha Inicio").reset_index(drop=True)
    df_out.drop(columns=["__semana"], inplace=True, errors="ignore")

    total = len(df_out)
    print("   [STATS] Distribucin Tipo_Evolucion (nuevos registros):")
    for k, v in df_out["Tipo_Evolucion"].value_counts().items():
        print(f"      {k:<20} {v:>7,}  ({v/total*100:.1f}%)")

    return df_out


#  Pipeline principal 

def procesar_datos(excel_bytes: bytes, folder_id: str, watermark=None) -> pd.DataFrame | None:
    """
    Procesa el Excel semanal de forma incremental.

    Args:
        excel_bytes: bytes del archivo Excel descargado de Drive
        folder_id:   ID de carpeta Drive donde vive el parquet de backup
        watermark:   datetime con la fecha mxima en Neon (None = primera carga)

    Returns:
        DataFrame con los registros procesados, o None si no haba datos nuevos.
    """
    print("=" * 60)
    print("  ETL INCREMENTAL - data_processor")
    print("=" * 60)

    #  Fase 0: Leer y filtrar Excel 
    print("\n Leyendo Excel...")
    try:
        df = pd.read_excel(io.BytesIO(excel_bytes), skiprows=1)
    except Exception as exc:
        print(f"[ERROR] Error leyendo Excel: {exc}")
        return None

    df["Fecha Inicio"] = pd.to_datetime(df["Fecha Inicio"], errors="coerce")

    if watermark:
        print(f"📅 Watermark: {watermark} - filtrando solo registros posteriores...")
        df = df[df["Fecha Inicio"] > watermark].copy()
    else:
        print("📅 Sin watermark - procesando todo el archivo (primera carga).")

    if df.empty:
        print("⚠️  No hay registros nuevos. Nada que hacer.")
        return None

    # Eliminar duplicados exactos dentro del lote nuevo
    rows_antes = len(df)
    df.drop_duplicates(inplace=True)
    rows_despues = len(df)
    if rows_antes > rows_despues:
        print(f"   [CUT]  Se eliminaron {rows_antes - rows_despues:,} duplicados exactos del Excel.")

    print(f"[OK] Registros nuevos a procesar: {len(df):,}")

    #  Fase 0b: Backup Crudo (Incremental) — TEMPORALMENTE DESACTIVADO
    # Guardamos los datos tal cual vienen del Excel (post-watermark) en 2025_historico_v2.parquet
    # print(f"\n[DIR] Actualizando backup CRUDO ({FILE_RAW})...")
    # service_raw = get_drive_service()
    # try:
    #     df_raw_prev = download_parquet_as_df(service_raw, FILE_RAW, folder_id)
    #     if df_raw_prev is not None and not df_raw_prev.empty:
    #         # Asegurar dtypes compatibles para el concat
    #         for col in df_raw_prev.columns:
    #             if col in df.columns:
    #                 try:
    #                     df[col] = df[col].astype(df_raw_prev[col].dtype)
    #                 except:
    #                     pass
    #         df_raw_completo = pd.concat([df_raw_prev, df], ignore_index=True)
    #         print(f"   Prev: {len(df_raw_prev):,} + Nuevo: {len(df):,} = Total Raw: {len(df_raw_completo):,}")
    #     else:
    #         df_raw_completo = df
    #         print("   Parquet crudo previo vacio - creando desde cero.")
    #     upload_df_as_parquet(service_raw, df_raw_completo, FILE_RAW, folder_id)
    #     print("   ✅ Parquet crudo actualizado en Drive")
    #     del df_raw_prev, df_raw_completo; gc.collect()
    # except Exception as exc:
    #     print(f"   ⚠️  Error actualizando backup CRUDO: {exc}")
    print("\n[DIR] Backup CRUDO omitido temporalmente.")

    # Normalizacin bsica de texto antes del geo
    cols_obj = df.select_dtypes(include=["object"]).columns
    for col in cols_obj:
        df[col] = df[col].astype(str).str.strip()

    #  Fase 1: Geo 
    df = calcular_comunas(df)

    #  Fase 2: Limpieza y categorizacin 
    df = limpiar_y_categorizar(df)

    #  Fase 3: Tipo_Evolucion con historial real de Neon 
    print("\n🔗 Cargando estado historico desde Neon...")
    engine = get_neon_engine()
    ultima_comuna, dni_seen = _build_estado_historico(engine)
    df = clasificar_tipo_evolucion_incremental(df, ultima_comuna, dni_seen)

    #  Fase 3b: Apariciones acumuladas por DNI 
    print("\n📊 Calculando apariciones...")
    df = _calcular_apariciones_incremental(df)

    #  Fase 4: Append a Neon 
    print(f"\n💾 Append a Neon ({TABLE_NEON})...")
    # Normalizar nombres de columnas de fecha recurso al snake_case canónico
    # antes de parsear. El Excel las trae con espacios y mayúsculas mixtas.
    _cols_map = {c.lower().replace(" ", "_"): c for c in df.columns}
    for _canon in ("recurso_fecha_asignacion", "recurso_fecha_liberado", "recurso_arribo"):
        _real = _cols_map.get(_canon)
        if _real and _real != _canon:
            df.rename(columns={_real: _canon}, inplace=True)

    # Re-parsear columnas de fecha a datetime64 justo antes del COPY.
    COLS_FECHA = [
        "Fecha Inicio", "Fecha Fin",
        "recurso_fecha_asignacion", "recurso_fecha_liberado", "recurso_arribo",
    ]
    for col in COLS_FECHA:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    engine_copy = get_neon_engine()
    append_neon_copy(engine_copy, df, TABLE_NEON)
    engine_copy.dispose()
    engine.dispose()
    #  Fase 5: Actualizar parquet de backup en Drive 
    print(f"\n📁 Actualizando backup Drive ({FILE_PARQUET})...")
    service = get_drive_service()
    try:
        df_prev = download_parquet_as_df(service, FILE_PARQUET, folder_id)

        def _coerce_to_reference_dtypes(df_in: pd.DataFrame, ref: pd.DataFrame) -> pd.DataFrame:
            df_out = df_in.copy()
            for col in ref.columns:
                if col not in df_out.columns:
                    continue
                target = ref[col].dtype

                if pd.api.types.is_datetime64_any_dtype(target):
                    df_out[col] = pd.to_datetime(df_out[col], errors="coerce")
                    continue
                if pd.api.types.is_integer_dtype(target):
                    df_out[col] = pd.to_numeric(df_out[col], errors="coerce").astype("Int64")
                    continue
                if pd.api.types.is_float_dtype(target):
                    df_out[col] = pd.to_numeric(df_out[col], errors="coerce")
                    continue
                if pd.api.types.is_bool_dtype(target):
                    df_out[col] = df_out[col].astype("boolean")
                    continue
                if pd.api.types.is_object_dtype(target) or pd.api.types.is_string_dtype(target):
                    df_out[col] = df_out[col].astype("string")
                    continue

                try:
                    df_out[col] = df_out[col].astype(target)
                except Exception:
                    df_out[col] = df_out[col].astype("string")

            return df_out

        if df_prev is not None and not df_prev.empty:
            df_prev["Fecha Inicio"] = pd.to_datetime(df_prev["Fecha Inicio"], errors="coerce")
            if "id suceso" in df_prev.columns:
                df_prev["id suceso"] = df_prev["id suceso"].astype("string")
            if "id suceso" in df.columns:
                df["id suceso"] = df["id suceso"].astype("string")

            df = _coerce_to_reference_dtypes(df, df_prev)
            df_completo = pd.concat([df_prev, df], ignore_index=True)
            print(f"   Prev: {len(df_prev):,} + Nuevo: {len(df):,} = Total: {len(df_completo):,}")
            del df_prev; gc.collect()

            # Guardia: si algún registro del lote nuevo quedó con DNI_categorizado nulo
            # después del concat (ej: coerción de tipos), re-aplicar categorización.
            if 'DNI_categorizado' in df_completo.columns and 'Persona DNI' in df_completo.columns:
                _null_vals = {'none', 'nan', '<na>', '', 'null'}
                mask_null = df_completo['DNI_categorizado'].apply(
                    lambda v: v is None
                    or (isinstance(v, float) and np.isnan(v))
                    or str(v).strip().lower() in _null_vals
                )
                if mask_null.any():
                    print(f"   [FIX] {mask_null.sum():,} filas con DNI_categorizado nulo — re-categorizando...")
                    df_completo.loc[mask_null] = limpiar_y_categorizar_dni_v3(
                        df_completo[mask_null].copy(),
                        'Persona DNI', columna_salida='DNI_categorizado', crear_motivo=False
                    )
                    df_completo['DNI_categorizado'] = df_completo['DNI_categorizado'].astype(str).str.strip()
                    still_null = df_completo['DNI_categorizado'].apply(
                        lambda v: str(v).strip().lower() in _null_vals
                    ).sum()
                    print(f"   [FIX] Corregidos. Restantes sin dato original: {still_null:,}")
        else:
            df_completo = df
            print("   Parquet previo vacio - creando desde cero.")

        if "id suceso" in df_completo.columns:
            df_completo["id suceso"] = df_completo["id suceso"].astype("string")

        upload_df_as_parquet(service, df_completo, FILE_PARQUET, folder_id)
        print("   ✅ Parquet actualizado en Drive")
        del df_completo; gc.collect()

    except Exception as exc:
        print(f"   [WARN] Error actualizando parquet Drive: {exc}")
        print("      Los datos SI estan en Neon. Drive queda desactualizado.")

    print(f"\n[FIN] ETL completado. {len(df):,} registros nuevos en Neon.")
    return df