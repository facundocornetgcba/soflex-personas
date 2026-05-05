"""
Migración one-time de cierres viejos → nuevo esquema canónico.

1. Remapea valores de resultado en historico_limpio (Neon).
2. Nulifica viejos cierres sin equivalente en el nuevo esquema.
3. Recalcula categoria_final, nivel_contacto, contacto, brinda_datos para TODO el histórico.
4. Espeja cambios al parquet de backup en Drive.

Ejecutar una sola vez.
"""

import gc
import io
import pandas as pd
import numpy as np
from sqlalchemy import text as sa_text

from core.db_connections import get_neon_engine
from core.drive_manager import download_parquet_as_df, get_drive_service, upload_df_as_parquet
from core.transformations import (
    limpiar_texto_cierre,
    mapear_categoria_con_reglas,
    obtener_nivel_contacto,
    obtener_niveles,
)

TABLE_NEON   = "historico_limpio"
FILE_PARQUET = "2025_historico_limpio.parquet"
FOLDER_ID_DB = "1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t"

# Mapeo viejo → nuevo para UPDATE directo en Neon.
# Claves = valor exacto en la columna resultado (raw, sin limpiar).
# Valores = nuevo valor canónico (o None para nulificar).
REMAP_RESULTADO = {
    "01-Traslado efectivo a CIS":                  "01. Traslado efectivo a CIS",
    "02-Acepta CIS pero no hay vacante":            "06. Acepta CIS pero no hay vacante",
    "03-Se activa Protocolo de Salud Mental":       "13. Derivación a SAME por salud mental",
    "05-Derivación a SAME":                         "12. Derivación a SAME por deterioro físico visible",
    "07-Rechaza entrevista y se retira del lugar":  "07. Se realiza entrevista y se retira del lugar",
    "10-Derivación a Espacio Público":              "15. Derivación a Ordenamiento Urbano",
    "11-No se contacta y se observan pertenencias": "16. No se observan personas y hay pertenencias",
    "12–No se contacta y no se observan pertenencias": "17. No se observan personas ni pertenencias",
    "13-Mendicidad (menores de edad)":              "18. Mendicidad",
    "15-Sin cubrir":                                "19. Sin cubrir",
    "16-Desestimado (cartas 911 u otras áreas)":   "20. Desestimado",
    # Nulificar (sin equivalente en nuevo esquema)
    "04-Traslado/acompañamiento a otros efectores": None,
    "06-Se realiza entrevista":                     None,
    "08-Imposibilidad de abordaje por consumo":     None,
    "09-Rechaza entrevista y se queda en el lugar": None,
    "14-No se encuentra en situación de calle":     None,
    "NEGATIVO":                                     None,
}

CHUNK_SIZE = 50_000
YEAR_FILTER = "2026-01-01"   # recalc + NULL-ify solo registros >= esta fecha


def _recalcular_columnas_derivadas(engine):
    """Lee registros >= YEAR_FILTER, recalcula categorias, escribe via temp table + UPDATE."""
    print(f"\n[RECALC] Leyendo historico Neon (>= {YEAR_FILTER})...")
    with engine.connect() as conn:
        df = pd.read_sql(
            sa_text(
                f'SELECT "Id Suceso", resultado, cierre_supervisor '
                f'FROM "{TABLE_NEON}" '
                f'WHERE "Fecha Inicio" >= \'{YEAR_FILTER}\''
            ),
            conn,
        )
    print(f"   {len(df):,} filas leidas.")

    # Replicar la logica de limpiar_y_categorizar sin tocar las columnas de entrada
    sup = df["cierre_supervisor"].astype(object)
    res = df["resultado"].astype(object)
    mask_sup_nulo = sup.isna()
    df["cierre_texto"] = np.where(mask_sup_nulo, res, sup)

    df["texto_limpio"]    = df["cierre_texto"].apply(limpiar_texto_cierre)
    df["categoria_final"] = df["texto_limpio"].apply(mapear_categoria_con_reglas)
    df["nivel_contacto"]  = df["categoria_final"].apply(obtener_nivel_contacto)
    niveles = df["categoria_final"].apply(obtener_niveles)
    df["contacto"]        = niveles.apply(lambda x: x[0])
    df["brinda_datos"]    = niveles.apply(lambda x: x[1])

    sin_match = (df["categoria_final"] == "sin_match").sum()
    print(f"   sin_match: {sin_match:,} / {len(df):,}")

    recalc = df[["Id Suceso", "cierre_texto", "texto_limpio",
                 "categoria_final", "nivel_contacto", "contacto", "brinda_datos"]].copy()
    recalc.columns = ["id_suceso", "cierre_texto", "texto_limpio",
                      "categoria_final", "nivel_contacto", "contacto", "brinda_datos"]
    # id_suceso como texto para evitar mismatch de tipo con la columna en Neon
    recalc["id_suceso"] = recalc["id_suceso"].astype(str)

    print("\n[RECALC] Escribiendo tabla temporal en Neon...")
    TEMP = "_recalc_cierres_tmp"

    with engine.connect() as conn:
        raw = conn.connection
        with raw.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{TEMP}"')
            cur.execute(
                f'''CREATE TEMP TABLE "{TEMP}" (
                    id_suceso TEXT,
                    cierre_texto TEXT,
                    texto_limpio TEXT,
                    categoria_final TEXT,
                    nivel_contacto TEXT,
                    contacto TEXT,
                    brinda_datos TEXT
                )'''
            )
            for start in range(0, len(recalc), CHUNK_SIZE):
                chunk = recalc.iloc[start:start + CHUNK_SIZE]
                buf = io.StringIO()
                chunk.to_csv(buf, index=False, header=False, na_rep=r"\N")
                buf.seek(0)
                cur.copy_expert(
                    f'COPY "{TEMP}" (id_suceso, cierre_texto, texto_limpio, '
                    f'categoria_final, nivel_contacto, contacto, brinda_datos) '
                    f'FROM STDIN WITH (FORMAT CSV, NULL \'\\N\')',
                    buf,
                )
            print(f"   COPY OK ({len(recalc):,} filas en temp table)")

            cur.execute(
                f'''UPDATE "{TABLE_NEON}" h
                    SET cierre_texto    = u.cierre_texto,
                        texto_limpio    = u.texto_limpio,
                        categoria_final = u.categoria_final,
                        nivel_contacto  = u.nivel_contacto,
                        contacto        = u.contacto,
                        brinda_datos    = u.brinda_datos
                    FROM "{TEMP}" u
                    WHERE h."Id Suceso"::text = u.id_suceso'''
            )
            print(f"   UPDATE filas: {cur.rowcount:,}")
            raw.commit()
    print("   [OK] UPDATE de columnas derivadas completo.")

    return df  # devolver para espejar en parquet


def migrar_neon(engine):
    print("\n[NEON] Remapeando valores de resultado...")
    with engine.connect() as conn:
        for viejo, nuevo in REMAP_RESULTADO.items():
            if nuevo is None:
                # NULL-ify solo en 2026+ para evitar disco lleno en Neon free tier
                # (los registros pre-2026 sin mapeo quedan con el valor viejo pero
                #  categoria_final se mantiene de la carga original — no afecta reportes 2026)
                r = conn.execute(sa_text(
                    f'UPDATE "{TABLE_NEON}" SET resultado = NULL '
                    f'WHERE resultado = :v '
                    f'AND "Fecha Inicio" >= \'{YEAR_FILTER}\''
                ), {"v": viejo})
            else:
                # Remap a nuevo valor: aplica a todo el histórico (ya corrió parcialmente)
                # WHERE también filtra para no re-tocar filas ya migradas
                r = conn.execute(sa_text(
                    f'UPDATE "{TABLE_NEON}" SET resultado = :n '
                    f'WHERE resultado = :v'
                ), {"n": nuevo, "v": viejo})
            if r.rowcount:
                label = nuevo if nuevo else "NULL (2026+)"
                print(f"   '{viejo}' → '{label}': {r.rowcount:,} filas")
        conn.commit()
    print("   [OK] Remap de resultado completo.")

    df_recalc = _recalcular_columnas_derivadas(engine)
    return df_recalc


def migrar_parquet(df_neon: pd.DataFrame):
    print("\n[PARQUET] Descargando backup Drive...")
    service = get_drive_service()
    df = download_parquet_as_df(service, FILE_PARQUET, FOLDER_ID_DB)
    if df is None or df.empty:
        print("   Parquet vacio o no encontrado. Saltando.")
        return

    # Buscar columna resultado (case-insensitive)
    col_res = next(
        (c for c in df.columns if c.lower().replace(" ", "_") == "resultado"),
        None,
    )
    if col_res is None:
        print("   Columna resultado no encontrada en parquet.")
        return

    # Aplicar el mismo remap de valores
    fecha_col = next((c for c in df.columns if "fecha" in c.lower() and "inicio" in c.lower()), None)
    df_fechas = pd.to_datetime(df[fecha_col], errors="coerce") if fecha_col else None

    for viejo, nuevo in REMAP_RESULTADO.items():
        mask = df[col_res] == viejo
        if nuevo is None and df_fechas is not None:
            # NULL solo en 2026+ (mismo criterio que Neon)
            mask = mask & (df_fechas >= pd.Timestamp(YEAR_FILTER))
        if mask.any():
            df.loc[mask, col_res] = nuevo  # None → NaN en pandas

    # Parchar columnas derivadas desde df_neon (ya recalculadas)
    id_col = next(
        (c for c in df.columns if c.lower().replace(" ", "").replace("_", "") == "idsuceso"
         and "asociado" not in c.lower()),
        None,
    )
    if id_col:
        # Dedup por Id Suceso antes de construir lookup (puede haber duplicados)
        lookup = (
            df_neon.drop_duplicates(subset=["Id Suceso"], keep="last")
            .set_index("Id Suceso")[
                ["cierre_texto", "texto_limpio", "categoria_final",
                 "nivel_contacto", "contacto", "brinda_datos"]
            ]
        )
        id_series = pd.to_numeric(df[id_col], errors="coerce")
        for col_new in lookup.columns:
            df[col_new] = id_series.map(lookup[col_new])
        print(f"   Columnas derivadas parcheadas via '{id_col}'.")
    else:
        print("   [WARN] 'Id Suceso' no encontrado en parquet — columnas derivadas NO parcheadas.")

    upload_df_as_parquet(service, df, FILE_PARQUET, FOLDER_ID_DB)
    print("   [OK] Parquet actualizado en Drive.")
    del df; gc.collect()


def baseline_count(engine):
    """Imprime distribución de resultado antes/después para auditoría."""
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            f'SELECT resultado, COUNT(*) AS n FROM "{TABLE_NEON}" '
            f'GROUP BY resultado ORDER BY n DESC'
        )).fetchall()
    return rows


if __name__ == "__main__":
    print("=" * 60)
    print("  MIGRACION CIERRES - nuevo esquema canónico")
    print("=" * 60)

    engine = get_neon_engine()

    print("\n[PRE] Distribución de resultado (top 30):")
    pre = baseline_count(engine)
    for val, n in pre[:30]:
        print(f"   {n:>6,}  {val}")

    df_neon = migrar_neon(engine)

    print("\n[POST] Distribución de resultado (top 30):")
    post = baseline_count(engine)
    for val, n in post[:30]:
        print(f"   {n:>6,}  {val}")

    sin_match_count = next((n for v, n in post if v == "sin_match"), 0)
    print(f"\n[CHECK] categoria_final='sin_match': {sin_match_count:,} filas")

    engine.dispose()

    migrar_parquet(df_neon)

    print("\n[OK] Migración de cierres completa.")
