#!/usr/bin/env python3
"""
reporte_semanal_origen.py
=========================
Genera reporte_semanal_origen.html con:
  1. Tabla semanal: intervenciones × origen × nivel de contacto (con %, filas colapsables)
  2. Gráficos de entrevista y resultado final (filtro de comuna aplica a todo)

Filtro base: Id Suceso Asociado == 0.
Fuente: parquet en Google Drive.
"""

import sys
import json
import datetime
import base64
import os

if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd
import numpy as np

from core.drive_manager import get_drive_service, download_parquet_as_df

# ── Configuración ──────────────────────────────────────────────────────────────

FOLDER_ID_DB = "1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t"
FILE_PARQUET = "2025_historico_limpio.parquet"
OUTPUT_HTML  = "reporte_semanal_origen.html"
N_SEMANAS    = 8    # últimas N semanas en tabla; 0 = todas
N_SEM_CHART  = 8    # semanas en gráficos
DEDUP_ID_SUCESO = False   # True = contar solo el primer registro por id suceso único

ORIGEN_A_GRUPO: dict[str, str] = {
    "SIN TECHO":                             "Vecino / ONG / PST",
    "ESPONTANEO":                            "Vecino / ONG / PST",
    "VECINO":                                "Vecino / ONG / PST",
    "ONG":                                   "Vecino / ONG / PST",
    "GESTION COLABORATIVA":                  "Vecino / ONG / PST",
    "911":                                   "Gobierno Ciudad (Coop.)",
    "ORGANISMOS PUBLICOS":                   "Gobierno Ciudad (Coop.)",
    "DESDE BAP/MDR":                         "Gobierno Ciudad (Coop.)",
    "PUNTO POLITICO/INGRESO POR FUERA 108":  "Gobierno Ciudad (Coop.)",
    "SUBTE":                                 "Gobierno Ciudad (Coop.)",
    "ESPACIOS PUBLICOS":                     "Gobierno Ciudad (Coop.)",
    "JUDICIALES":                            "Gobierno Ciudad (Coop.)",
    "BOTI":                                  "Gobierno Ciudad (Coop.)",
    "MONITOREO 108":                         "Gobierno Ciudad (Coop.)",
    "CAJEROS":                               "Gobierno Ciudad (Coop.)",
    "SEGUIMIENTO":                           "Gobierno Ciudad (Coop.)",
    "ADICCIONES":                            "Gobierno Ciudad (Coop.)",
    "NIÑA/NIÑO":                             "Gobierno Ciudad (Coop.)",
}

ORIGEN_EXCLUIR = {"EQUIVOCADO", "CORTAN"}

GRUPO_ORDEN = [
    "Gobierno Ciudad (Coop.)",
    "Vecino / ONG / PST",
]

NIVEL_ORDEN = [
    "Se contacta",
    "No se contacta",
    "Sin cubrir",
    "Desestimado",
    "Seguimiento",
]

# ── Constantes para gráficos ───────────────────────────────────────────────────

_REALIZA_ENTREVISTA_CATS = {
    "traslado efectivo a cis",
    "acepta cis pero no hay vacante",
    "se activa protocolo de salud mental",
    "derivacion a same",
    "traslado/acompanamiento a otros efectores",
    "mendicidad (menores de edad)",
    "derivacion a centro de nnnya",
    "se realiza entrevista",
}
_DERIVADO_CATS = {
    "traslado efectivo a cis",
    "acepta cis pero no hay vacante",
    "se activa protocolo de salud mental",
    "derivacion a same",
    "traslado/acompanamiento a otros efectores",
    "mendicidad (menores de edad)",
    "derivacion a centro de nnnya",
}
_DNI_INVALIDOS_STR = {
    "NO BRINDO/NO VISIBLE", "NO BRINDO", "NO VISIBLE", "CONTACTO EXTRANJERO",
    "S/D", "X", "NAN", "nan", "NaN", "", " ", "NONE", "None",
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
}

# ── Helpers tabla ───────────────────────────────────────────────────────────────

def mapear_origen(valor) -> str:
    if pd.isna(valor):
        return "Sin origen"
    s = str(valor).strip().upper()
    if s in ORIGEN_EXCLUIR:
        return "__excluir__"
    return ORIGEN_A_GRUPO.get(s, f"Otro ({str(valor).strip()})")


def nivel_display(nivel_contacto_val, categoria_final_val) -> str:
    cat = str(categoria_final_val).strip().lower() if not pd.isna(categoria_final_val) else ""
    if cat == "sin cubrir":
        return "Sin cubrir"
    niv = str(nivel_contacto_val).strip() if not pd.isna(nivel_contacto_val) else ""
    if niv == "Se contacta":
        return "Se contacta"
    if niv == "No se contacta":
        return "No se contacta"
    if niv == "Desestimado":
        return "Desestimado"
    return "Seguimiento"


def counts_by_semana(df: pd.DataFrame, mask: pd.Series, semanas) -> list:
    return (
        df.loc[mask, "semana"]
        .value_counts()
        .reindex(semanas, fill_value=0)
        .tolist()
    )


# ── Helpers gráficos ────────────────────────────────────────────────────────────

def _es_dni_valido(val) -> bool:
    if val is None:
        return False
    s = str(val).strip()
    if s in _DNI_INVALIDOS_STR or len(s) < 6:
        return False
    if len(set(s)) == 1:
        return False
    return s.replace(".", "").replace("-", "").isdigit()


def _clasificar_entrevista(cat, dni_val) -> str:
    cat_str = str(cat).strip().lower() if not pd.isna(cat) else ""
    if cat_str in _REALIZA_ENTREVISTA_CATS:
        return "Brinda DNI" if _es_dni_valido(dni_val) else "No brinda"
    return "No realiza entrevista"


def _clasificar_resultado(cat) -> str:
    cat_str = str(cat).strip().lower() if not pd.isna(cat) else ""
    if cat_str in _DERIVADO_CATS:
        return "Derivado"
    if cat_str == "rechaza entrevista y se retira del lugar":
        return "Se retira"
    if cat_str in {"rechaza entrevista y se queda en el lugar", "se realiza entrevista"}:
        return "Se queda"
    if cat_str == "derivacion a espacio publico":
        return "Espacio público"
    return "Otros"


# ── Preparación del DataFrame ──────────────────────────────────────────────────

def preparar_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df["Fecha Inicio"] = pd.to_datetime(df["Fecha Inicio"], errors="coerce")
    df = df.dropna(subset=["Fecha Inicio"])
    df["semana"] = df["Fecha Inicio"].dt.to_period("W-SUN").dt.start_time

    id_num = pd.to_numeric(df["Id Suceso Asociado"], errors="coerce").fillna(0)
    antes = len(df)
    df = df[id_num == 0].copy()
    print(f"   [id_suceso=0] {len(df):,} / {antes:,}  (excluidos: {antes-len(df):,})")

    df["grupo_origen"] = df["Origen"].apply(mapear_origen)
    df = df[df["grupo_origen"] != "__excluir__"].copy()

    # Excluir registros sin categoría válida (sin_match y error de soflex)
    CATS_EXCLUIR = {"sin_match", "error de soflex"}
    antes_cat = len(df)
    df = df[~df["categoria_final"].isin(CATS_EXCLUIR)].copy()
    print(f"   [cat_excluida] {len(df):,} / {antes_cat:,}  (excluidos: {antes_cat-len(df):,})")

    # Deduplicar por id suceso (queda el primer registro por suceso único)
    if DEDUP_ID_SUCESO:
        # Buscar la columna sin importar mayúsculas/minúsculas
        col_id = next(
            (c for c in df.columns if c.lower().replace(" ", "").replace("_", "") == "idsuceso"
             and "asociado" not in c.lower()),
            None
        )
        if col_id:
            antes_dedup = len(df)
            df = df.sort_values("Fecha Inicio").drop_duplicates(subset=[col_id], keep="first").copy()
            print(f"   [dedup_id]    {len(df):,} / {antes_dedup:,}  (excluidos: {antes_dedup-len(df):,})  col='{col_id}'")
        else:
            print(f"   [dedup_id]    COLUMNA NO ENCONTRADA. Columnas disponibles: {list(df.columns)}")

    df["tipo_carta_norm"] = df["Tipo Carta"].str.upper().str.strip()
    df["nivel_norm"] = df.apply(
        lambda r: nivel_display(r.get("nivel_contacto"), r.get("categoria_final")),
        axis=1
    )

    def norm_comuna(v):
        if pd.isna(v):
            return None
        try:
            f = float(v)
            return "14.5" if f == 14.5 else str(int(f))
        except Exception:
            return None
    df["comuna_key"] = df["comuna_calculada"].apply(norm_comuna)

    return df


# ── Estructura de filas ────────────────────────────────────────────────────────

def build_row_structure(df: pd.DataFrame) -> list:
    rows = []

    rows.append({"label": "Intervenciones totales", "indent": 0, "type": "total",
                 "key": ("total",), "parent_ridx": None})

    rows.append({"label": "Automáticas", "indent": 1, "type": "subtotal",
                 "key": ("auto",), "parent_ridx": 0})
    auto_ridx = len(rows) - 1

    mask_auto = df["tipo_carta_norm"] == "AUTOMATICA"

    # Totales de nivel para todas las automáticas
    for nivel in NIVEL_ORDEN:
        if not (mask_auto & (df["nivel_norm"] == nivel)).any():
            continue
        rows.append({"label": nivel, "indent": 2, "type": "auto_nivel",
                     "key": ("auto_nivel", nivel), "parent_ridx": auto_ridx})

    # Desglose por grupo de origen
    for grupo in GRUPO_ORDEN:
        mask_g = mask_auto & (df["grupo_origen"] == grupo)
        if not mask_g.any():
            continue
        grupo_ridx = len(rows)
        rows.append({"label": grupo, "indent": 2, "type": "grupo",
                     "key": ("grupo", grupo), "parent_ridx": auto_ridx})
        for nivel in NIVEL_ORDEN:
            if not (mask_g & (df["nivel_norm"] == nivel)).any():
                continue
            rows.append({"label": nivel, "indent": 3, "type": "nivel",
                         "key": ("nivel", grupo, nivel), "parent_ridx": grupo_ridx})

    rows.append({"label": "Manuales", "indent": 1, "type": "subtotal_manual",
                 "key": ("manual",), "parent_ridx": 0})
    manual_ridx = len(rows) - 1

    mask_manual = df["tipo_carta_norm"] != "AUTOMATICA"

    for nivel in NIVEL_ORDEN:
        if not (mask_manual & (df["nivel_norm"] == nivel)).any():
            continue
        niv_ridx = len(rows)
        rows.append({"label": nivel, "indent": 2, "type": "manual_nivel",
                     "key": ("manual_nivel", nivel), "parent_ridx": manual_ridx})
        if nivel == "No se contacta":
            mask_nsc = mask_manual & (df["nivel_norm"] == nivel)
            for grupo in GRUPO_ORDEN:
                if not (mask_nsc & (df["grupo_origen"] == grupo)).any():
                    continue
                rows.append({"label": grupo, "indent": 3, "type": "manual_nsc_origen",
                             "key": ("manual_nsc_origen", grupo), "parent_ridx": niv_ridx})

    return rows


# ── Cómputo de valores ─────────────────────────────────────────────────────────

def compute_vals_for_df(df_sub: pd.DataFrame, row_structure: list, semanas) -> list[list]:
    mask_auto   = df_sub["tipo_carta_norm"] == "AUTOMATICA"
    mask_manual = df_sub["tipo_carta_norm"] != "AUTOMATICA"
    TRUE_MASK   = pd.Series(True, index=df_sub.index)

    result = []
    for row in row_structure:
        k = row["key"]
        if k[0] == "total":
            mask = TRUE_MASK
        elif k[0] == "auto":
            mask = mask_auto
        elif k[0] == "manual":
            mask = mask_manual
        elif k[0] == "auto_nivel":
            _, nivel = k
            mask = mask_auto & (df_sub["nivel_norm"] == nivel)
        elif k[0] == "grupo":
            _, grupo = k
            mask = mask_auto & (df_sub["grupo_origen"] == grupo)
        elif k[0] == "nivel":
            _, grupo, nivel = k
            mask = mask_auto & (df_sub["grupo_origen"] == grupo) & (df_sub["nivel_norm"] == nivel)
        elif k[0] == "manual_nivel":
            _, nivel = k
            mask = mask_manual & (df_sub["nivel_norm"] == nivel)
        else:  # manual_nsc_origen
            _, grupo = k
            mask = mask_manual & (df_sub["nivel_norm"] == "No se contacta") & (df_sub["grupo_origen"] == grupo)
        result.append(counts_by_semana(df_sub, mask, semanas))
    return result


COMUNAS_DESTACADAS = {"2", "13", "14", "14.5"}

def build_all_data(df: pd.DataFrame, row_structure: list, semanas) -> tuple:
    data = {}
    print("   Calculando 'Todas las comunas'...")
    data["todas"] = compute_vals_for_df(df, row_structure, semanas)

    for c in COMUNAS_DESTACADAS:
        df_c = df[df["comuna_key"] == c]
        if not df_c.empty:
            data[c] = compute_vals_for_df(df_c, row_structure, semanas)
            print(f"      Comuna {c}: {len(df_c):,} registros")

    df_resto = df[~df["comuna_key"].isin(COMUNAS_DESTACADAS)]
    print(f"   Calculando 'Resto de la ciudad': {len(df_resto):,} registros...")
    data["resto"] = compute_vals_for_df(df_resto, row_structure, semanas)

    return data


# ── Datos para gráficos (por comuna) ──────────────────────────────────────────

def compute_contacto_breakdown_weekly(df: pd.DataFrame, global_weeks: list) -> dict:
    """Calcula breakdown de contacto para el df dado usando las semanas globales."""
    COL_NIVEL = "nivel_contacto"
    COL_CAT   = "categoria_final"
    COL_TIPO  = "tipo_carta_norm"
    COL_DNI   = "DNI_categorizado"

    weeks_str = [w.strftime("%d/%m") for w in global_weeks]
    empty = {"weeks": weeks_str, "auto_entrevista": {}, "manual_entrevista": {},
             "auto_resultado": {}, "manual_resultado": {}}

    if COL_NIVEL not in df.columns or COL_CAT not in df.columns:
        return empty

    df_c = df[df[COL_NIVEL] == "Se contacta"].copy()
    if df_c.empty:
        ENT = {"Brinda DNI": [0]*len(global_weeks), "No brinda": [0]*len(global_weeks),
               "No realiza entrevista": [0]*len(global_weeks)}
        RES = {g: [0]*len(global_weeks) for g in ["Derivado", "Se retira", "Se queda", "Espacio público", "Otros"]}
        return {"weeks": weeks_str,
                "auto_entrevista": ENT, "manual_entrevista": dict(ENT),
                "auto_resultado": RES, "manual_resultado": dict(RES)}

    dni_col = df_c[COL_DNI] if COL_DNI in df_c.columns else pd.Series([""] * len(df_c), index=df_c.index)
    df_c = df_c.copy()
    df_c["_ent"] = [_clasificar_entrevista(cat, dni) for cat, dni in zip(df_c[COL_CAT], dni_col)]
    df_c["_res"] = df_c[COL_CAT].apply(_clasificar_resultado)

    ENT_GRUPOS = ["Brinda DNI", "No brinda", "No realiza entrevista"]
    RES_GRUPOS = ["Derivado", "Se retira", "Se queda", "Espacio público", "Otros"]

    result = {"weeks": weeks_str}

    for tipo_carta, key in [("AUTOMATICA", "auto"), ("MANUAL", "manual")]:
        df_tipo = df_c[df_c[COL_TIPO] == tipo_carta]

        ent = {}
        for g in ENT_GRUPOS:
            weekly = (
                df_tipo[df_tipo["_ent"] == g]
                .groupby("semana").size()
                .reindex(global_weeks, fill_value=0)
            )
            ent[g] = [int(v) for v in weekly]
        result[f"{key}_entrevista"] = ent

        res = {}
        for g in RES_GRUPOS:
            weekly = (
                df_tipo[df_tipo["_res"] == g]
                .groupby("semana").size()
                .reindex(global_weeks, fill_value=0)
            )
            res[g] = [int(v) for v in weekly]
        result[f"{key}_resultado"] = res

    return result


def build_all_chart_data(df: pd.DataFrame, global_weeks: list) -> dict:
    chart_data = {}
    print("   Charts 'Todas las comunas'...")
    chart_data["todas"] = compute_contacto_breakdown_weekly(df, global_weeks)
    for c in COMUNAS_DESTACADAS:
        df_c = df[df["comuna_key"] == c]
        if not df_c.empty:
            chart_data[c] = compute_contacto_breakdown_weekly(df_c, global_weeks)
    df_resto = df[~df["comuna_key"].isin(COMUNAS_DESTACADAS)]
    chart_data["resto"] = compute_contacto_breakdown_weekly(df_resto, global_weeks)
    return chart_data


# ── Generación HTML ────────────────────────────────────────────────────────────

def generar_html(row_structure: list, all_data: dict,
                 semanas, chart_data_all: dict, last_update: str,
                 img_tag: str = "", n_sem_default: int = 8) -> str:

    sem_labels = [s.strftime("%d/%m") for s in semanas]
    sem_iso    = [s.strftime("%Y-%m-%d") for s in semanas]
    th_semanas = "".join(
        f'<th class="wh" data-widx="{i}" data-wdate="{sem_iso[i]}">{lbl}</th>'
        for i, lbl in enumerate(sem_labels)
    )

    TYPE_CLASS = {
        "total":             "r-total",
        "subtotal":          "r-sub-auto",
        "subtotal_manual":   "r-sub-manual",
        "auto_nivel":        "r-auto-nivel",
        "grupo":             "r-grupo",
        "nivel":             "r-nivel",
        "manual_nivel":      "r-manual-nivel",
        "manual_nsc_origen": "r-manual-nsc-origen",
    }
    PAD = {0: 10, 1: 22, 2: 36, 3: 52}

    parent_set = {r["parent_ridx"] for r in row_structure if r["parent_ridx"] is not None}

    rows_html = ""
    for ridx, row in enumerate(row_structure):
        ind       = row["indent"]
        cls       = TYPE_CLASS.get(row["type"], "")
        pad       = PAD.get(ind, 10)
        is_parent = ridx in parent_set
        tds       = "".join(f'<td class="vc" data-widx="{i}"></td>' for i in range(len(semanas)))

        if is_parent:
            lbl_html = (
                f'<span class="tog" data-ridx="{ridx}">▼</span>'
                f'<span class="lbl">{row["label"]}</span>'
            )
        else:
            lbl_html = f'<span class="lbl">{row["label"]}</span>'

        pr_attr = f'data-parent="{row["parent_ridx"]}"' if row["parent_ridx"] is not None else ''

        rows_html += (
            f'<tr class="{cls}" data-ridx="{ridx}" {pr_attr}>'
            f'<td class="lc" style="padding-left:{pad}px">{lbl_html}</td>'
            f'{tds}</tr>\n'
        )

    parent_ridx_js = json.dumps([r["parent_ridx"] for r in row_structure])

    options_html = '<option value="todas">Todas las comunas</option>\n'
    for c, label in [("2", "Comuna 2"), ("13", "Comuna 13"), ("14", "Comuna 14"), ("14.5", "Palermo Norte (14.5)"), ("resto", "Resto de la ciudad")]:
        if c in all_data:
            options_html += f'<option value="{c}">{label}</option>\n'

    json_data       = json.dumps(all_data, separators=(",", ":"))
    json_charts_all = json.dumps(chart_data_all, ensure_ascii=False)
    json_sem_iso    = json.dumps(sem_iso)

    def chart_card(canvas_id, title):
        return (f'<div class="chart-card">'
                f'<div class="chart-title">{title}</div>'
                f'<div class="chart-wrap"><canvas id="{canvas_id}"></canvas></div>'
                f'</div>')

    charts_html = (
        f'<div class="charts-section">'
        f'<div class="res-tbl-card">'
        f'<div class="chart-title">Entrevista por Semana — Detalle (Se Contacta)</div>'
        f'<div class="tbl-wrap" id="entrevista-tbl-wrap"></div>'
        f'</div>'
        f'<div class="res-tbl-card">'
        f'<div class="chart-title">Resultado Final por Semana — Detalle (Se Contacta)</div>'
        f'<div class="tbl-wrap" id="resultado-tbl-wrap"></div>'
        f'</div>'
        f'<div class="charts-grid">'
        f'{chart_card("entrevistaAutoChart",   "Entrevista — Automáticas (Se Contacta)")}'
        f'{chart_card("entrevistaManualChart", "Entrevista — Manuales (Se Contacta)")}'
        f'{chart_card("resultadoAutoChart",    "Resultado Final — Automáticas (Se Contacta)")}'
        f'{chart_card("resultadoManualChart",  "Resultado Final — Manuales (Se Contacta)")}'
        f'</div>'
        f'</div>'
    )

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Reporte Semanal — Origen y Contacto</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@3.9.1/dist/chart.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0"></script>
<script src="https://cdn.tailwindcss.com"></script>
<script>
  tailwind.config = {{
    corePlugins: {{ preflight: false }},
    theme: {{
      extend: {{
        colors: {{
          'ba-yellow': '#FFD100',
          'ba-grey': '#333333',
        }}
      }}
    }}
  }}
</script>
<style>
/* ── Reset & base ─────────────────────────────────────────────────────────── */
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',Arial,sans-serif;background:#f0f4f4;color:#1f2937}}

/* ── Contenido interior ───────────────────────────────────────────────────── */
.inner{{padding:0 18px}}

/* ── Card de tabla ────────────────────────────────────────────────────────── */
.card{{
  background:#fff;
  border-radius:12px;
  box-shadow:0 2px 14px rgba(0,0,0,.09);
  overflow:hidden;
  margin-bottom:24px;
  border:1px solid #d4e8e4;
}}

/* Encabezado de card con el filtro */
.card-hdr{{
  background:#3D8B7A;
  padding:12px 18px;
  display:flex;
  align-items:center;
  justify-content:space-between;
  flex-wrap:wrap;
  gap:8px;
}}
.card-hdr-title{{
  color:#fff;
  font-size:13px;
  font-weight:700;
  letter-spacing:.3px;
}}
.filter-wrap{{display:flex;align-items:center;gap:8px}}
.filter-wrap label{{font-size:12px;color:#d4f0eb;font-weight:600;white-space:nowrap}}
#sel-comuna{{
  background:#2d6b5e;
  color:#fff;
  border:1px solid #8BE3D9;
  border-radius:8px;
  padding:6px 12px;
  font-size:12px;
  cursor:pointer;
  outline:none;
  min-width:180px;
  appearance:none;
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%238BE3D9' d='M6 8L1 3h10z'/%3E%3C/svg%3E");
  background-repeat:no-repeat;
  background-position:right 10px center;
  padding-right:28px;
}}
#sel-comuna option{{background:#1E2B37}}

.tbl-wrap{{overflow-x:auto}}

/* ── Tabla ────────────────────────────────────────────────────────────────── */
table{{width:100%;border-collapse:collapse;min-width:600px}}
thead th{{
  position:sticky;top:0;z-index:2;
  background:#1E2B37;
  color:#A7F3D0;
  font-size:11px;font-weight:700;
  text-transform:uppercase;letter-spacing:.4px;
  padding:10px 7px;text-align:center;
  border-bottom:2px solid #3D8B7A;
  white-space:nowrap;
}}
thead th.lh{{text-align:left;min-width:240px;color:#fff;font-size:12px}}
th.wh{{min-width:82px}}

/* ── Filas por tipo ──────────────────────────────────────────────────────────── */

/* Total general */
tr.r-total      td{{background:#1E2B37;color:#fff;font-weight:700;font-size:13px;border-bottom:2px solid #3D8B7A}}

/* ── Familia AUTOMÁTICAS (verde/teal) ── */
tr.r-sub-auto   td{{background:#145C48;color:#A7F3D0;font-weight:700;font-size:12.5px;letter-spacing:.2px}}
/* Niveles resumen (Se contacta, No se contacta…) dentro de Automáticas */
tr.r-auto-nivel td{{background:#1D7055;color:#6EE7B7;font-size:11.5px;font-style:italic}}
/* Grupos de origen (Gobierno Ciudad, Vecino/ONG/PST) */
tr.r-grupo      td{{background:#2E9E82;color:#ECFDF5;font-weight:600;font-size:12px}}
/* Niveles dentro de cada grupo */
tr.r-nivel      td{{background:#F0FAF7;color:#1A3D33;font-size:11.5px}}
tr.r-nivel:nth-child(even) td{{background:#E4F6F1}}

/* ── Familia MANUALES (azul/índigo) ── */
tr.r-sub-manual td{{background:#1E3669;color:#BAD4F8;font-weight:700;font-size:12.5px;letter-spacing:.2px}}
/* Niveles dentro de Manuales */
tr.r-manual-nivel      td{{background:#2A4F96;color:#93C5FD;font-size:11.5px;font-style:italic}}
/* Origen dentro de No se contacta */
tr.r-manual-nsc-origen td{{background:#EEF2FF;color:#1E3669;font-size:11.5px}}
tr.r-manual-nsc-origen:nth-child(even) td{{background:#E0E7FF}}

tr:not(.r-total):hover td{{filter:brightness(.94)}}

td{{padding:7px 8px;border-bottom:1px solid rgba(0,0,0,.05)}}
td.lc{{text-align:left;white-space:nowrap}}
td.vc{{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}}
tr.r-total             td.vc{{color:#fff;font-weight:700}}
tr.r-sub-auto          td.vc{{color:#A7F3D0;font-weight:700}}
tr.r-auto-nivel        td.vc{{color:#6EE7B7}}
tr.r-grupo             td.vc{{color:#ECFDF5;font-weight:600}}
tr.r-nivel             td.vc{{color:#2D6B55}}
tr.r-sub-manual        td.vc{{color:#BAD4F8;font-weight:700}}
tr.r-manual-nivel      td.vc{{color:#93C5FD}}
tr.r-manual-nsc-origen td.vc{{color:#2A4F96}}
.pct{{font-size:.82em;opacity:.68;margin-left:2px}}

/* Columnas ocultas por filtro de fecha */
th.col-hidden, td.col-hidden {{ display:none }}

/* Toggle colapso */
.tog{{
  display:inline-block;width:18px;text-align:center;
  cursor:pointer;font-size:10px;opacity:.8;
  transition:transform .15s;user-select:none;
}}
.tog.collapsed{{transform:rotate(-90deg)}}
tr.hidden{{display:none}}

@keyframes flash{{0%{{opacity:.3}}100%{{opacity:1}}}}
.flash td.vc{{animation:flash .22s ease-out}}

/* Footer */
.footer{{
  background:#f0faf8;
  border-top:1px solid #c5e8e1;
  padding:10px 18px;
  display:flex;gap:18px;flex-wrap:wrap;
  font-size:11px;color:#4a7a70;
}}
.footer strong{{color:#1E2B37}}

/* ── Gráficos ─────────────────────────────────────────────────────────────── */
.charts-section{{margin-top:8px}}
.charts-grid{{display:grid;grid-template-columns:repeat(2,1fr);gap:16px}}
@media(max-width:900px){{.charts-grid{{grid-template-columns:1fr}}}}
.chart-card{{
  background:#fff;border-radius:12px;
  box-shadow:0 2px 10px rgba(0,0,0,.08);
  border:1px solid #d4e8e4;overflow:hidden;
}}
.chart-title{{
  background:#3D8B7A;color:#fff;
  font-size:12px;font-weight:700;
  padding:10px 14px;letter-spacing:.3px;
}}
.chart-wrap{{padding:12px;height:280px;position:relative}}

/* Tablas resultado/entrevista */
.res-tbl-card{{
  background:#fff;border-radius:12px;margin-top:16px;
  box-shadow:0 2px 10px rgba(0,0,0,.08);
  border:1px solid #d4e8e4;overflow:hidden;
}}
/* Total general */
tr.r-total-res          td{{background:#1E2B37;color:#fff;font-weight:700;font-size:12.5px;border-bottom:2px solid #3D8B7A}}
tr.r-total-res          td.vc{{color:#A7F3D0;font-weight:700}}
tr.r-niv-res            td{{background:#f5faf9;color:#374151;font-size:11.5px}}
tr.r-niv-res:nth-child(even) td{{background:#e8f4f1}}
tr.r-niv-res            td.vc{{color:#6b7280}}
/* Automáticas — verde */
tr.r-sub-res-auto       td{{background:#145C48;color:#A7F3D0;font-weight:700;font-size:12px;letter-spacing:.2px}}
tr.r-sub-res-auto       td.vc{{color:#A7F3D0;font-weight:700}}
tr.r-niv-res-auto       td{{background:#F0FAF7;color:#1A3D33;font-size:11.5px}}
tr.r-niv-res-auto:nth-child(even) td{{background:#E4F6F1}}
tr.r-niv-res-auto       td.vc{{color:#2D6B55}}
/* Manuales — azul */
tr.r-sub-res-manual     td{{background:#1E3669;color:#BAD4F8;font-weight:700;font-size:12px;letter-spacing:.2px}}
tr.r-sub-res-manual     td.vc{{color:#BAD4F8;font-weight:700}}
tr.r-niv-res-manual     td{{background:#EEF2FF;color:#1E3669;font-size:11.5px}}
tr.r-niv-res-manual:nth-child(even) td{{background:#E0E7FF}}
tr.r-niv-res-manual     td.vc{{color:#2A4F96}}
</style>
</head>
<body>
<div class="wrap">

  <!-- ── Header ── -->
  <header class="sticky top-0 z-50 flex w-full h-24 bg-[#1E2B37] font-sans shadow-md">
        <!-- Teal Bar Wrapper -->
        <div class="flex-grow bg-gradient-to-r from-[#8BE3D9] to-[#80E0D6] rounded-tr-[3rem] flex mr-4 relative items-center">

            <!-- Yellow Section (Tab) -->
            <div class="bg-ba-yellow h-full w-full lg:w-1/2 rounded-tr-[3rem] px-6 flex items-center gap-6 relative z-10 shadow-sm">
                 <!-- Title block -->
                 <div class="flex flex-col justify-center min-w-0">
                     <h1 class="text-sm font-bold text-ba-grey uppercase tracking-wide leading-tight whitespace-nowrap">
                         INDICADORES CLAVE - RED DE ATENCI&Oacute;N
                     </h1>
                     <p class="text-[10px] text-gray-600 font-medium mt-0.5 whitespace-nowrap">Solo sucesos sin evento asociado (suceso asociado = 0)</p>
                 </div>

                 <!-- Vertical Divider & Date -->
                 <div class="flex items-center gap-3 border-l border-gray-400 pl-4 h-1/2 shrink-0">
                     <div class="flex flex-col text-xs font-semibold text-gray-800">
                          <!-- Placeholders que el regex reemplazar abajo -->
                          <div>Actualizado: {last_update}</div>
                          <div class="text-gray-600">Semana: {semanas[0].strftime('%d %b')} - {semanas[-1].strftime('%d %b')}</div>
                     </div>
                 </div>
            </div>

            <!-- Teal Decoration (Empty space to the right of yellow acts as the teal bar) -->
        </div>

        <!-- Logo Area -->
        <div class="w-24 md:w-32 flex items-center justify-center shrink-0 pr-4">
             {img_tag}
        </div>
    </header>

  <div class="inner">
    <!-- ── Card tabla principal ── -->
    <div class="card">
      <div class="card-hdr">
        <div class="card-hdr-title">Dimensión / Semana</div>
        <div class="filter-wrap">
          <label for="sel-comuna">Filtrar por comuna:</label>
          <select id="sel-comuna">
            {options_html}
          </select>
        </div>
        <div class="filter-wrap" style="gap:6px">
          <label style="font-size:12px;color:#d4f0eb;font-weight:600;white-space:nowrap">Desde:</label>
          <select id="date-from" style="background:#2d6b5e;color:#fff;border:1px solid #8BE3D9;border-radius:8px;padding:5px 8px;font-size:12px;cursor:pointer;outline:none;min-width:130px"></select>
          <label style="font-size:12px;color:#d4f0eb;font-weight:600;white-space:nowrap">Hasta:</label>
          <select id="date-to"   style="background:#2d6b5e;color:#fff;border:1px solid #8BE3D9;border-radius:8px;padding:5px 8px;font-size:12px;cursor:pointer;outline:none;min-width:130px"></select>
        </div>
      </div>
      <div class="tbl-wrap">
        <table>
          <thead>
            <tr>
              <th class="lh">Dimensión</th>
              {th_semanas}
            </tr>
          </thead>
          <tbody id="tbl-body">
{rows_html}          </tbody>
        </table>
      </div>
      <div class="footer">
        <span><strong>Fuente:</strong> {FILE_PARQUET}</span>
        <span><strong>Semanas:</strong> {len(semanas)} · la más antigua a la izquierda</span>
        <span><strong>%:</strong> cada fila respecto a su fila padre · clic en fila para expandir/colapsar</span>
        <span><strong>Gobierno Ciudad:</strong> 911 · Org. Públicos · BAP/MDR · BOTI · Subte · Esp. Públicos · Judiciales · Cajeros · Monitoreo 108 · Adicciones · Seguimiento · Niña/Niño · Punto Político</span>
        <span><strong>Vecino/ONG/PST:</strong> Vecino · ONG · Gestión Colaborativa · Sin Techo · Espontáneo</span>
      </div>
    </div>

    {charts_html}
  </div>
</div>

<script>
// ── Datos ─────────────────────────────────────────────────────────────────────
const DATA       = {json_data};
const CHART_DATA = {json_charts_all};
const PARENT_IDX = {parent_ridx_js};
const WEEK_DATES = {json_sem_iso};
const N_SEM_DEFAULT = {n_sem_default};

// ── Filtro de columnas por fecha ──────────────────────────────────────────────
const selFrom = document.getElementById('date-from');
const selTo   = document.getElementById('date-to');

function fmtWeekLabel(iso) {{
  const [y, m, d] = iso.split('-');
  return `${{d}}/${{m}}/${{y}}`;
}}

// Poblar ambos selects con todas las semanas disponibles
WEEK_DATES.forEach((d, i) => {{
  const lbl = fmtWeekLabel(d);
  selFrom.add(new Option(lbl, d));
  selTo.add(new Option(lbl, d));
}});

function applyWeekFilter() {{
  const from = selFrom.value;
  const to   = selTo.value;
  WEEK_DATES.forEach((d, i) => {{
    const visible = d >= from && d <= to;
    document.querySelectorAll(`[data-widx="${{i}}"]`).forEach(el => {{
      el.classList.toggle('col-hidden', !visible);
    }});
  }});
}}

// Inicializar con las últimas N_SEM_DEFAULT semanas
(function initDateFilter() {{
  if (WEEK_DATES.length === 0) return;
  const fromIdx = Math.max(0, WEEK_DATES.length - N_SEM_DEFAULT);
  selFrom.selectedIndex = fromIdx;
  selTo.selectedIndex   = WEEK_DATES.length - 1;
  applyWeekFilter();
}})();

selFrom.addEventListener('change', applyWeekFilter);
selTo.addEventListener('change', applyWeekFilter);

// ── Tabla: render ─────────────────────────────────────────────────────────────
const sel   = document.getElementById('sel-comuna');
const tbody = document.getElementById('tbl-body');
const trows = Array.from(tbody.querySelectorAll('tr[data-ridx]'));

function fmtCell(n, colIdx, vals, parentIdx) {{
  if (n === 0) return '–';
  let s = n.toLocaleString('es-AR');
  if (parentIdx !== null && parentIdx !== undefined) {{
    const p = vals[parentIdx][colIdx];
    if (p > 0) s += '<span class="pct">(' + Math.round(n * 100 / p) + '%)</span>';
  }}
  return s;
}}

function renderRows(vals) {{
  trows.forEach(tr => {{
    const ridx      = parseInt(tr.dataset.ridx, 10);
    const rowVals   = vals[ridx];
    const parentIdx = PARENT_IDX[ridx];
    tr.querySelectorAll('td.vc').forEach((td, i) => {{
      td.innerHTML = fmtCell(rowVals[i], i, vals, parentIdx);
    }});
  }});
}}

renderRows(DATA['todas']);

// ── Tabla: colapso ────────────────────────────────────────────────────────────
const collapsed = new Set();

function isHidden(ridx) {{
  let p = PARENT_IDX[ridx];
  while (p !== null && p !== undefined) {{
    if (collapsed.has(p)) return true;
    p = PARENT_IDX[p];
  }}
  return false;
}}

function applyVisibility() {{
  trows.forEach(tr => {{
    const ridx = parseInt(tr.dataset.ridx, 10);
    tr.classList.toggle('hidden', isHidden(ridx));
  }});
}}

document.querySelectorAll('.tog').forEach(tog => {{
  tog.addEventListener('click', e => {{
    e.stopPropagation();
    const ridx = parseInt(tog.dataset.ridx, 10);
    if (collapsed.has(ridx)) {{
      collapsed.delete(ridx);
      tog.classList.remove('collapsed');
    }} else {{
      collapsed.add(ridx);
      tog.classList.add('collapsed');
    }}
    applyVisibility();
  }});
}});

// ── Filtro de comuna ──────────────────────────────────────────────────────────
sel.addEventListener('change', function() {{
  const key  = this.value;
  const vals = DATA[key];
  if (!vals) return;

  renderRows(vals);
  trows.forEach(tr => {{
    tr.classList.remove('flash');
    void tr.offsetWidth;
    tr.classList.add('flash');
  }});

  updateCharts(key);
  renderEntrevistaTable(key);
  renderResultadoTable(key);
}});

// ── Gráficos ──────────────────────────────────────────────────────────────────
if (typeof ChartDataLabels !== 'undefined') {{
  Chart.register(ChartDataLabels);
}}

const CHART_INSTANCES = {{}};
const CHART_KEYS = {{
  'entrevistaAutoChart':   cd => cd.auto_entrevista,
  'entrevistaManualChart': cd => cd.manual_entrevista,
  'resultadoAutoChart':    cd => cd.auto_resultado,
  'resultadoManualChart':  cd => cd.manual_resultado,
}};

const ENT_COLORS = ['#10B981', '#3B82F6', '#F97316'];
const RES_COLORS = ['#8B5CF6', '#EF4444', '#F59E0B', '#06B6D4', '#9CA3AF'];

function initContactoChart(canvasId, countsByGroup, colors) {{
  const el = document.getElementById(canvasId);
  if (!el) return;
  const weeks = CHART_DATA.todas.weeks;
  const datasets = Object.entries(countsByGroup).map(([label, data], i) => ({{
    label, data, backgroundColor: colors[i % colors.length],
  }}));
  CHART_INSTANCES[canvasId] = new Chart(el.getContext('2d'), {{
    type: 'bar',
    data: {{ labels: weeks, datasets }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      scales: {{
        x: {{ stacked: true, grid: {{ display: false }} }},
        y: {{ stacked: true, beginAtZero: true }},
      }},
      plugins: {{
        legend: {{ position: 'top', labels: {{ font: {{ size: 11 }} }} }},
        tooltip: {{ mode: 'index', intersect: false }},
        datalabels: {{
          color: 'white', font: {{ weight: 'bold', size: 10 }},
          formatter: v => v > 0 ? v : ''
        }}
      }}
    }},
    plugins: [{{
      id: 'totals',
      afterDatasetsDraw: chart => {{
        const ctx2 = chart.ctx;
        chart.data.labels.forEach((_, idx) => {{
          let total = 0;
          chart.data.datasets.forEach(ds => total += (ds.data[idx] || 0));
          if (total > 0) {{
            const meta = chart.getDatasetMeta(chart.data.datasets.length - 1);
            ctx2.fillStyle = '#1f2937'; ctx2.font = 'bold 11px sans-serif';
            ctx2.textAlign = 'center';
            ctx2.fillText(total, meta.data[idx].x, meta.data[idx].y - 5);
          }}
        }});
      }}
    }}]
  }});
}}

function updateCharts(key) {{
  const cd = CHART_DATA[key];
  if (!cd) return;
  Object.entries(CHART_KEYS).forEach(([canvasId, getter]) => {{
    const chart = CHART_INSTANCES[canvasId];
    if (!chart) return;
    const groups = getter(cd);
    chart.data.datasets.forEach(ds => {{
      if (groups[ds.label] !== undefined) ds.data = groups[ds.label];
    }});
    chart.update('none');
  }});
}}

// ── Tabla entrevista ──────────────────────────────────────────────────────────
const ENT_GRUPOS = ["Brinda DNI", "No brinda", "No realiza entrevista"];
const entTblWrap = document.getElementById('entrevista-tbl-wrap');

entTblWrap.addEventListener('click', e => {{
  const tog = e.target.closest('.tog-ent');
  if (!tog) return;
  const tr   = tog.closest('tr');
  const tipo = tr.dataset.tipo;
  const isCollapsed = tog.classList.contains('collapsed');
  tog.classList.toggle('collapsed', !isCollapsed);
  entTblWrap.querySelectorAll(`tr[data-parent-tipo="${{tipo}}"]`).forEach(child => {{
    child.classList.toggle('hidden', !isCollapsed);
  }});
}});

function renderEntrevistaTable(key) {{
  const cd = CHART_DATA[key];
  if (!cd || !cd.weeks || !cd.weeks.length) return;
  const weeks = cd.weeks;

  function weekTotals(entData) {{
    return weeks.map((_, i) => ENT_GRUPOS.reduce((s, g) => s + (entData[g]?.[i] || 0), 0));
  }}

  function fmtE(n, parentVals, i) {{
    if (n === 0) return '–';
    let s = n.toLocaleString('es-AR');
    const p = parentVals[i];
    if (p > 0) s += '<span class="pct">(' + Math.round(n * 100 / p) + '%)</span>';
    return s;
  }}

  function combinedEntData() {{
    const combined = {{}};
    ENT_GRUPOS.forEach(g => {{
      const a = cd.auto_entrevista[g]   || weeks.map(() => 0);
      const m = cd.manual_entrevista[g] || weeks.map(() => 0);
      combined[g] = a.map((v, i) => v + (m[i] || 0));
    }});
    return combined;
  }}

  function buildEntTotalSection() {{
    const comb   = combinedEntData();
    const totals = weekTotals(comb);
    const totalTds = totals.map(v => `<td class="vc">${{v > 0 ? v.toLocaleString('es-AR') : '–'}}</td>`).join('');
    let html = `<tr class="r-total-res" data-tipo="total-ent">
      <td class="lc" style="padding-left:10px"><span class="tog-ent tog collapsed">▼</span><strong>Total contactados</strong></td>
      ${{totalTds}}</tr>`;
    ENT_GRUPOS.forEach(g => {{
      const vals = comb[g] || weeks.map(() => 0);
      const tds  = vals.map((v, i) => `<td class="vc">${{fmtE(v, totals, i)}}</td>`).join('');
      html += `<tr class="r-niv-res hidden" data-parent-tipo="total-ent">
        <td class="lc" style="padding-left:36px">${{g}}</td>${{tds}}</tr>`;
    }});
    return html;
  }}

  function buildEntSection(entData, label, tipo, variant) {{
    const totals   = weekTotals(entData);
    const totalTds = totals.map(v => `<td class="vc">${{v > 0 ? v.toLocaleString('es-AR') : '–'}}</td>`).join('');
    let html = `<tr class="r-sub-res-${{variant}}" data-tipo="${{tipo}}">
      <td class="lc" style="padding-left:10px"><span class="tog-ent tog collapsed">▼</span>${{label}}</td>
      ${{totalTds}}</tr>`;
    ENT_GRUPOS.forEach(g => {{
      const vals = entData[g] || weeks.map(() => 0);
      const tds  = vals.map((v, i) => `<td class="vc">${{fmtE(v, totals, i)}}</td>`).join('');
      html += `<tr class="r-niv-res-${{variant}} hidden" data-parent-tipo="${{tipo}}">
        <td class="lc" style="padding-left:36px">${{g}}</td>${{tds}}</tr>`;
    }});
    return html;
  }}

  const ths = weeks.map(w => `<th class="wh">${{w}}</th>`).join('');
  entTblWrap.innerHTML = `
    <table>
      <thead><tr>
        <th class="lh">Entrevista (Se Contacta)</th>${{ths}}
      </tr></thead>
      <tbody>
        ${{buildEntTotalSection()}}
        ${{buildEntSection(cd.auto_entrevista,   'Automáticas', 'auto-ent',    'auto')}}
        ${{buildEntSection(cd.manual_entrevista, 'Manuales',    'manual-ent',  'manual')}}
      </tbody>
    </table>`;
}}

// ── Tabla resultado ───────────────────────────────────────────────────────────
const RES_GRUPOS = ["Derivado", "Se retira", "Se queda", "Espacio público", "Otros"];
const resTblWrap = document.getElementById('resultado-tbl-wrap');

resTblWrap.addEventListener('click', e => {{
  const tog = e.target.closest('.tog-res');
  if (!tog) return;
  const tr   = tog.closest('tr');
  const tipo = tr.dataset.tipo;
  const isCollapsed = tog.classList.contains('collapsed');
  tog.classList.toggle('collapsed', !isCollapsed);
  resTblWrap.querySelectorAll(`tr[data-parent-tipo="${{tipo}}"]`).forEach(child => {{
    child.classList.toggle('hidden', !isCollapsed);
  }});
}});

function renderResultadoTable(key) {{
  const cd = CHART_DATA[key];
  if (!cd || !cd.weeks || !cd.weeks.length) return;
  const weeks = cd.weeks;

  function weekTotals(resData) {{
    return weeks.map((_, i) => RES_GRUPOS.reduce((s, g) => s + (resData[g]?.[i] || 0), 0));
  }}

  function fmtR(n, parentVals, i) {{
    if (n === 0) return '–';
    let s = n.toLocaleString('es-AR');
    const p = parentVals[i];
    if (p > 0) s += '<span class="pct">(' + Math.round(n * 100 / p) + '%)</span>';
    return s;
  }}

  function combinedData() {{
    const combined = {{}};
    RES_GRUPOS.forEach(g => {{
      const a = cd.auto_resultado[g]   || weeks.map(() => 0);
      const m = cd.manual_resultado[g] || weeks.map(() => 0);
      combined[g] = a.map((v, i) => v + (m[i] || 0));
    }});
    return combined;
  }}

  function buildTotalSection() {{
    const comb = combinedData();
    const totals = weekTotals(comb);
    const totalTds = totals.map(v => `<td class="vc">${{v > 0 ? v.toLocaleString('es-AR') : '–'}}</td>`).join('');
    let html = `<tr class="r-total-res" data-tipo="total">
      <td class="lc" style="padding-left:10px"><span class="tog-res tog collapsed">▼</span><strong>Total contactados</strong></td>
      ${{totalTds}}</tr>`;
    RES_GRUPOS.forEach(g => {{
      const vals = comb[g] || weeks.map(() => 0);
      const tds  = vals.map((v, i) => `<td class="vc">${{fmtR(v, totals, i)}}</td>`).join('');
      html += `<tr class="r-niv-res hidden" data-parent-tipo="total">
        <td class="lc" style="padding-left:36px">${{g}}</td>${{tds}}</tr>`;
    }});
    return html;
  }}

  function buildSection(resData, label, tipo, variant) {{
    const totals = weekTotals(resData);
    const totalTds = totals.map(v => `<td class="vc">${{v > 0 ? v.toLocaleString('es-AR') : '–'}}</td>`).join('');
    let html = `<tr class="r-sub-res-${{variant}}" data-tipo="${{tipo}}">
      <td class="lc" style="padding-left:10px"><span class="tog-res tog collapsed">▼</span>${{label}}</td>
      ${{totalTds}}</tr>`;
    RES_GRUPOS.forEach(g => {{
      const vals = resData[g] || weeks.map(() => 0);
      const tds  = vals.map((v, i) => `<td class="vc">${{fmtR(v, totals, i)}}</td>`).join('');
      html += `<tr class="r-niv-res-${{variant}} hidden" data-parent-tipo="${{tipo}}">
        <td class="lc" style="padding-left:36px">${{g}}</td>${{tds}}</tr>`;
    }});
    return html;
  }}

  const ths = weeks.map(w => `<th class="wh">${{w}}</th>`).join('');
  resTblWrap.innerHTML = `
    <table>
      <thead><tr>
        <th class="lh">Resultado Final (Se Contacta)</th>${{ths}}
      </tr></thead>
      <tbody>
        ${{buildTotalSection()}}
        ${{buildSection(cd.auto_resultado,   'Automáticas', 'auto',   'auto')}}
        ${{buildSection(cd.manual_resultado, 'Manuales',    'manual', 'manual')}}
      </tbody>
    </table>`;
}}

// Inicializar con datos de "todas"
const cd0 = CHART_DATA.todas;
initContactoChart('entrevistaAutoChart',   cd0.auto_entrevista,   ENT_COLORS);
initContactoChart('entrevistaManualChart', cd0.manual_entrevista, ENT_COLORS);
initContactoChart('resultadoAutoChart',    cd0.auto_resultado,    RES_COLORS);
initContactoChart('resultadoManualChart',  cd0.manual_resultado,  RES_COLORS);
renderEntrevistaTable('todas');
renderResultadoTable('todas');
</script>
</body>
</html>"""

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Reporte Semanal — Origen y Nivel de Contacto")
    print("=" * 60)

    print("\n🔑 Autenticando con Google Drive...")
    service = get_drive_service()

    print(f"\n⬇️  Descargando {FILE_PARQUET}...")
    df_raw = download_parquet_as_df(service, FILE_PARQUET, FOLDER_ID_DB)
    print(f"   {len(df_raw):,} registros")

    if df_raw.empty:
        print("❌ Parquet vacío.")
        return

    print("\n🔧 Preparando datos...")
    df = preparar_df(df_raw)
    print(f"   {len(df):,} registros tras filtros")

    semanas_todas = sorted(df["semana"].unique())
    semanas = semanas_todas  # Se embeben todas; JS filtra por rango de fechas
    print(f"\n📅 Tabla: {len(semanas)} semanas ({semanas[0].date()} → {semanas[-1].date()})")

    print("\n🔨 Construyendo estructura de filas...")
    row_structure = build_row_structure(df)
    print(f"   {len(row_structure)} filas")

    print("\n📊 Calculando datos de tabla por filtro...")
    all_data = build_all_data(df, row_structure, semanas)

    global_weeks = semanas_todas[-N_SEM_CHART:]
    print(f"\n📈 Calculando datos de gráficos por filtro ({N_SEM_CHART} semanas)...")
    chart_data_all = build_all_chart_data(df, global_weeks)

    last_update = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")

    logo_path = "logoba-removebg-preview.png"
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as image_file:
            logo_b64 = base64.b64encode(image_file.read()).decode('utf-8')
        img_tag = f'<img src="data:image/png;base64,{logo_b64}" alt="BA Logo" class="h-16 w-auto object-contain" />'
    else:
        img_tag = '<span class="text-white font-bold text-xl">BA</span>'

    print("\n🎨 Generando HTML...")
    html = generar_html(row_structure, all_data, semanas, chart_data_all, last_update, img_tag, n_sem_default=N_SEMANAS)

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅ Guardado: {OUTPUT_HTML}")
    print(f"   Filas: {len(row_structure)} · Semanas tabla: {len(semanas)} · "
          f"Semanas gráfico: {N_SEM_CHART} · Filtros: {len(all_data)}")


if __name__ == "__main__":
    main()
