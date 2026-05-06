import pandas as pd
import numpy as np
import datetime
import json
import re
import os
from data_processor import get_drive_service, download_parquet_as_df
from core.transformations import REALIZA_ENTREVISTA_CATS, DERIVADO_CATS

# --- CONFIGURACION ---
FOLDER_ID_DB = '1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t'
FILE_NAME_PARQUET = '2025_historico_limpio.parquet'
TEMPLATE_HTML_PATH = 'reporte_tablero.html'
OUTPUT_HTML_PATH = 'reporte_autom_bap.html'

AGENCIAS_EXCLUIR = {
    "DIPA I COMBATE", "MAPA DE RIESGO - SEGUIMIENTO",
    "MAPA DE REISGO - SEGUIMIENTO", "DIPA II ZABALA",
    "AREA OPERATIVA", "SALUD MENTAL",
}

# Aliases for local use (imported from transformations — canonical source of truth)
_REALIZA_ENTREVISTA_CATS = REALIZA_ENTREVISTA_CATS
_DERIVADO_CATS = DERIVADO_CATS
_DNI_INVALIDOS_STR = {
    "NO BRINDO/NO VISIBLE", "NO BRINDO", "NO VISIBLE", "CONTACTO EXTRANJERO",
    "S/D", "X", "NAN", "nan", "NaN", "", " ", "NONE", "None",
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
}

# =============================================================================
# LOGICA DE NEGOCIO
# =============================================================================

def clasificar_contacto(row):
    """Clasificación estricta de contactos usando nivel_contacto del ETL."""
    if row.get('estado') == 'PENDIENTE':
        c_val = row.get('comuna_calculada')
        es_priorizada = False
        try:
            if float(c_val) in (2.0, 13.0, 14.0, 1.5):
                es_priorizada = True
        except Exception:
            pass
        if not es_priorizada:
            return 'Sin cubrir'

    nivel = str(row.get('nivel_contacto', '')).strip()
    if nivel == 'Se contacta':
        return 'Se contacta'
    if nivel in ('No se contacta', 'Desestimado'):
        return 'No se contacta'
    if nivel == 'Sin cubrir':
        return 'Sin cubrir'

    # Fallback para registros históricos sin nivel_contacto calculado
    resultado = str(row.get('resultado', '')).lower()
    if any(phrase in resultado for phrase in ['no se contacta', 'no se observan', 'desestimado']):
        return 'No se contacta'
    if 'sin cubrir' in resultado:
        return 'Sin cubrir'
    return 'Sin dato'

def calculate_dni_evolution(df_base, target_comuna_id=2):
    COL_FECHA = "Fecha Inicio"
    COL_DNI = "DNI_categorizado"
    COL_COMUNA = "comuna_calculada"
    COL_EVO = "Tipo_Evolucion"

    # 1. Filtro por Comuna (None = todas las comunas)
    df = df_base.copy()
    if target_comuna_id is not None:
        def is_target_val(x):
            if pd.isna(x): return False
            try:
                val = float(str(x).strip().replace(",", "."))
                return val == float(target_comuna_id)
            except:
                if target_comuna_id == 14.5: return str(x).upper() == "PALERMO NORTE"
                if target_comuna_id == 13.5: return str(x).upper() == "BELGRANO"
                return False
        df = df[df[COL_COMUNA].apply(is_target_val)].copy()
    
    # 2. Normalizar Tipo_Evolucion
    if COL_EVO in df.columns:
        df[COL_EVO] = df[COL_EVO].replace("Nuevo repetido", "Nuevos")
    else:
        all_weeks = sorted(df_base[COL_FECHA].dt.to_period("W-SUN").dt.start_time.unique())[-8:]
        return [{"Semana": d, "recurrentes": 0, "migratorios": 0, "nuevos": 0} for d in all_weeks]

    # 3. Deduplicar DNI por Semana (Despues del filtro de Comuna)
    df["Semana"] = df[COL_FECHA].dt.to_period("W-SUN").dt.start_time
    df_sem = df.drop_duplicates(subset=["Semana", COL_DNI]).copy()

    # 4. Agrupar
    all_weeks = sorted(df_base[COL_FECHA].dt.to_period("W-SUN").dt.start_time.unique())[-8:]
    evo_pivot = (
        df_sem.groupby(["Semana", COL_EVO]).size()
        .unstack(fill_value=0)
        .reindex(all_weeks, fill_value=0)
    )

    for c in ["Recurrentes", "Migratorios", "Nuevos"]:
        if c not in evo_pivot.columns: evo_pivot[c] = 0

    res = []
    for sem, row in evo_pivot.iterrows():
        res.append({
            "Semana": sem,
            "recurrentes": int(row["Recurrentes"]),
            "migratorios": int(row["Migratorios"]),
            "nuevos": int(row["Nuevos"])
        })
    return res

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
    cat_str = str(cat).strip() if not pd.isna(cat) else ""
    if cat_str in _REALIZA_ENTREVISTA_CATS:
        return "Brinda DNI" if _es_dni_valido(dni_val) else "No brinda"
    return "No realiza entrevista"


def _clasificar_resultado(cat) -> str:
    from core.transformations import BUCKET_POR_CIERRE
    if pd.isna(cat):
        return "Cierres no identificables"
    cat_str = str(cat).strip()
    return BUCKET_POR_CIERRE.get(cat_str, "Cierres no identificables")


def compute_contacto_breakdown_weekly(df_base, n_semanas=8):
    """
    Para las cartas con nivel_contacto == 'Se contacta', calcula por semana:
    - Entrevista: Brinda DNI / No brinda / No realiza entrevista  (× Auto y Manual)
    - Resultado final: Derivado / Se retira / Se queda / Espacio público / Otros (× Auto y Manual)
    """
    COL_NIVEL = 'nivel_contacto'
    COL_CAT   = 'categoria_final'
    COL_TIPO  = 'Tipo Carta'
    COL_DNI   = 'DNI_categorizado'

    required = {COL_NIVEL, COL_CAT, COL_TIPO}
    if not required.issubset(df_base.columns):
        missing = required - set(df_base.columns)
        print(f"[WARN] compute_contacto_breakdown: columnas faltantes {missing}")
        return None

    df_c = df_base[df_base[COL_NIVEL] == 'Se contacta'].copy()
    df_c = df_c.loc[:, ~df_c.columns.duplicated()].copy()

    all_weeks = sorted(
        df_base['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time.unique()
    )[-n_semanas:]
    weeks_str = [w.strftime('%d %b').replace('.', '').title() for w in all_weeks]

    df_c['Semana'] = df_c['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time

    dni_col = df_c[COL_DNI] if COL_DNI in df_c.columns else pd.Series([''] * len(df_c), index=df_c.index)
    df_c['_ent_grupo'] = [
        _clasificar_entrevista(cat, dni)
        for cat, dni in zip(df_c[COL_CAT], dni_col)
    ]
    df_c['_res_grupo'] = df_c[COL_CAT].apply(_clasificar_resultado)

    ENT_GRUPOS = ["Brinda DNI", "No brinda", "No realiza entrevista"]
    RES_GRUPOS = [
        "DERIVACIÓN A DISPOSITIVO RED",
        "DERIVACION A SAME",
        "SE RETIRA VOLUNTARIAMENTE",
        "DERIVACIÓN A SEGURIDAD Y A ORDENAMIENTO URBANO",
        "DERIVACION UMBRAL CERO",
        "NO ERAN PSC",
    ]

    result = {'weeks': weeks_str}

    for tipo_carta, key in [('AUTOMATICA', 'auto'), ('MANUAL', 'manual')]:
        df_tipo = df_c[df_c[COL_TIPO] == tipo_carta]

        ent_counts = {}
        for g in ENT_GRUPOS:
            weekly = (
                df_tipo[df_tipo['_ent_grupo'] == g]
                .groupby('Semana').size()
                .reindex(all_weeks, fill_value=0)
            )
            ent_counts[g] = [int(v) for v in weekly]
        result[f'{key}_entrevista'] = ent_counts

        res_counts = {}
        for g in RES_GRUPOS:
            weekly = (
                df_tipo[df_tipo['_res_grupo'] == g]
                .groupby('Semana').size()
                .reindex(all_weeks, fill_value=0)
            )
            res_counts[g] = [int(v) for v in weekly]
        result[f'{key}_resultado'] = res_counts

    return result


_CIERRE_BUCKETS = ["SE DERIVA", "CASOS DE SALUD MENTAL", "SE RETIRA", "ESPACIO PUBLICO",
                   "ACEPTA CIS SIN VACANTE", "MENDICIDAD", "Cierres no identificables"]
_CIERRE_COLORS  = ["#10B981", "#EF4444", "#3B82F6", "#F59E0B", "#8B5CF6", "#EC4899", "#9CA3AF"]


def compute_cierres_breakdown_weekly(df_zona, n_semanas=8):
    """Breakdown por bucket PPT de los registros Se contacta, últimas n semanas."""
    from core.transformations import BUCKET_POR_CIERRE

    all_weeks = sorted(
        df_zona['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time.unique()
    )[-n_semanas:]
    weeks_str = [w.strftime('%d %b').replace('.', '').title() for w in all_weeks]

    df = df_zona[df_zona['nivel_contacto'] == 'Se contacta'].copy()
    if df.empty:
        return {
            'labels': weeks_str,
            'datasets': [{'label': b, 'data': [0] * n_semanas, 'backgroundColor': c}
                         for b, c in zip(_CIERRE_BUCKETS, _CIERRE_COLORS)]
        }

    df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time
    df['_bucket'] = df['categoria_final'].apply(
        lambda c: BUCKET_POR_CIERRE.get(str(c).strip(), 'Cierres no identificables')
        if pd.notna(c) else 'Cierres no identificables'
    )

    datasets = []
    for bucket, color in zip(_CIERRE_BUCKETS, _CIERRE_COLORS):
        weekly = (
            df[df['_bucket'] == bucket]
            .groupby('Semana').size()
            .reindex(all_weeks, fill_value=0)
        )
        datasets.append({'label': bucket, 'data': [int(v) for v in weekly], 'backgroundColor': color})

    return {'labels': weeks_str, 'datasets': datasets}


# =============================================================================
# GENERACION DE HTML INTERACTIVO Y CALCULOS GLOBALES
# =============================================================================

def get_stats_data_raw(df_base, comuna_filter_func, base_vals):
    """
    Devuelve un diccionario con los datos crudos para el frontend.
    """
    df = comuna_filter_func(df_base).copy()
    
    # 8 Semanas fijas
    all_weeks = sorted(df_base['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time.unique())[-8:]
    weeks_str = [w.strftime('%d %b').replace('.', '').title() for w in all_weeks]

    if df.empty:
        return {
            'weeks': weeks_str,
            'rows': [
                {'label': 'Intervenciones totales', 'base': base_vals[0], 'vals': [0]*8},
                {'label': 'Derivaciones CIS', 'base': base_vals[1], 'vals': [0]*8},
                {'label': 'Llamados 108', 'base': base_vals[2], 'vals': [0]*8},
                {'label': '% Se contacta', 'base': base_vals[3], 'vals': ["0% (0)"]*8},
                {'label': '% No se contacta', 'base': base_vals[4], 'vals': ["0% (0)"]*8},
                {'label': '% Sin cubrir', 'base': base_vals[5], 'vals': ["0% (0)"]*8},
            ]
        }

    df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time
    df['Categoria_contacto'] = df.apply(clasificar_contacto, axis=1)

    df_total_sem = df.groupby('Semana').size().reindex(all_weeks, fill_value=0)
    
    df_cis = df[df['categoria_final'] == '01. Traslado efectivo a CIS']
    df_cis_sem = df_cis.groupby('Semana').size().reindex(all_weeks, fill_value=0)

    df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
    df_auto_sem = df_auto.groupby('Semana').size().reindex(all_weeks, fill_value=0)

    df_auto_conteo = df_auto.groupby(['Semana', 'Categoria_contacto']).size().unstack(fill_value=0)
    for col in ['Se contacta', 'No se contacta', 'Sin cubrir']:
        if col not in df_auto_conteo.columns:
            df_auto_conteo[col] = 0
    df_auto_conteo = df_auto_conteo.reindex(all_weeks, fill_value=0)

    totales_auto = df_auto_conteo.sum(axis=1).replace(0, 1)
    df_pct = (df_auto_conteo.div(totales_auto, axis=0) * 100).round(0)

    rows = []
    
    def get_vals(series): return series.values.tolist()
    def get_comb(cat):
        return [f"{int(p)}% ({int(a)})" for p, a in zip(df_pct[cat].values, df_auto_conteo[cat].values)]

    rows.append({'label': 'Intervenciones totales', 'base': base_vals[0], 'vals': get_vals(df_total_sem)})
    rows.append({'label': 'Derivaciones CIS', 'base': base_vals[1], 'vals': get_vals(df_cis_sem)})
    rows.append({'label': 'Llamados 108', 'base': base_vals[2], 'vals': get_vals(df_auto_sem)})
    rows.append({'label': '% Se contacta', 'base': base_vals[3], 'vals': get_comb('Se contacta')})
    rows.append({'label': '% No se contacta', 'base': base_vals[4], 'vals': get_comb('No se contacta')})
    rows.append({'label': '% Sin cubrir', 'base': base_vals[5], 'vals': get_comb('Sin cubrir')})

    return {'weeks': weeks_str, 'rows': rows}

def main(df_externo=None):
    print(" Iniciando Generador de Dashboard Interactivo V2 (Fixed)...")

    if df_externo is not None:
        print("[INFO] Usando DataFrame externo (sin descarga adicional).")
        df = df_externo.copy()
    else:
        service = get_drive_service()
        print(f"[DOWNLOAD] Descargando {FILE_NAME_PARQUET}...")
        df = download_parquet_as_df(service, FILE_NAME_PARQUET, FOLDER_ID_DB)

    if df.empty: return

    # Normalizacin para transicin de nombres (Backward Compatibility)
    rename_logic = {
        "Resultado": "resultado",
        "Estado": "estado",
        "Agencia": "agencia",
        "Origen": "origen",
        "DNI_Categorizado": "DNI_categorizado",
        "Id Suceso": "id suceso"
    }
    df.rename(columns=rename_logic, inplace=True, errors='ignore')

    df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'], errors='coerce')
    df = df.dropna(subset=['Fecha Inicio'])
    last_update = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")

    # --- FILTROS GLOBALES (alineados con reporte_semanal_origen) ---

    # Filtro principal: solo sucesos sin evento asociado
    id_suceso_col = 'Id Suceso Asociado'
    if id_suceso_col in df.columns:
        id_num = pd.to_numeric(df[id_suceso_col], errors='coerce').fillna(0)
        antes = len(df)
        df = df[id_num == 0].copy()
        print(f"[FILTRO] Id Suceso Asociado==0: {len(df):,} / {antes:,} (excluidos: {antes-len(df):,})")

    # Excluir origenes invalidos
    df = df[~df['origen'].isin(['CORTAN', 'EQUIVOCADO'])].copy()

    # Excluir registros sin categoría válida (igual que reporte_semanal_origen)
    CATS_EXCLUIR = {"sin_match", "error de soflex"}
    if 'categoria_final' in df.columns:
        antes_cat = len(df)
        df = df[~df['categoria_final'].isin(CATS_EXCLUIR)].copy()
        print(f"[FILTRO] categoria_final excluidas: {len(df):,} / {antes_cat:,} (excluidos: {antes_cat-len(df):,})")

    print("[STATS] Calculando datos para TODAS las comunas...")
    
    all_data = {}
    
    # Bases HARDCODEADAS
    # Usare valores vacios "-" para las comunas que no son la 2, la 14 o el Resto
    base_dummy = ["-", "-", "-", "-", "-", "-"]
    base_c2 = ["341", "26",'175', "38% (66)", "53% (92)", "9% (16)"]
    
    # Base Total (Antiguamente Resto - Solicitado usar esta base para Total)
    base_total = ["4344", "341", "2798", "27% (782)", "25% (717)", "46% (1299)"]

    ZONAS_PRIORIZADAS = {2.0, 13.0, 14.0, 1.5}

    # Zonas priorizadas
    all_data['c2'] = get_stats_data_raw(
        df,
        lambda d: d[d['comuna_calculada'] == 2],
        base_c2
    )
    all_data['c13'] = get_stats_data_raw(
        df,
        lambda d: d[d['comuna_calculada'] == 13],
        base_dummy
    )
    all_data['c14'] = get_stats_data_raw(
        df,
        lambda d: d[d['comuna_calculada'] == 14],
        base_dummy
    )
    all_data['zona1a'] = get_stats_data_raw(
        df,
        lambda d: d[d['comuna_calculada'] == 1.5],
        base_dummy
    )

    # Resto de la ciudad (todo lo que NO es zona priorizada)
    all_data['resto'] = get_stats_data_raw(
        df,
        lambda d: d[~d['comuna_calculada'].isin(ZONAS_PRIORIZADAS)],
        base_dummy
    )

    # Total Ciudad
    all_data['total'] = get_stats_data_raw(df, lambda d: d, base_total)

    def prepare_chart_json(dni_data_list):
        return {
            "labels": [d["Semana"].strftime("%d %b") for d in dni_data_list],
            "datasets": [
               {"label": "Nuevos", "data": [d["nuevos"] for d in dni_data_list], "backgroundColor": "#10B981"},
               {"label": "Recurrentes", "data": [d["recurrentes"] for d in dni_data_list], "backgroundColor": "#3B82F6"},
               {"label": "Migratorios", "data": [d["migratorios"] for d in dni_data_list], "backgroundColor": "#F97316"}
            ]
        }

    print(" Calculando evolucin DNI para zonas priorizadas...")
    all_chart_data = {}
    all_chart_data['c2'] = prepare_chart_json(calculate_dni_evolution(df, target_comuna_id=2))
    all_chart_data['c13'] = prepare_chart_json(calculate_dni_evolution(df, target_comuna_id=13))
    all_chart_data['c14'] = prepare_chart_json(calculate_dni_evolution(df, target_comuna_id=14))
    all_chart_data['zona1a'] = prepare_chart_json(calculate_dni_evolution(df, target_comuna_id=1.5))

    # Resto: excluir zonas priorizadas
    df_resto_chart = df[~df['comuna_calculada'].isin(ZONAS_PRIORIZADAS)].copy()
    all_chart_data['resto'] = prepare_chart_json(calculate_dni_evolution(df_resto_chart, target_comuna_id=None))

    all_chart_data['total'] = prepare_chart_json(calculate_dni_evolution(df, target_comuna_id=None))
    # Alias para referencia de fecha en el header
    chart_json_c2 = all_chart_data['c2']

    print(" Calculando breakdown de cierres por zona...")
    _zona_dfs = {
        'c2':     df[df['comuna_calculada'] == 2],
        'c13':    df[df['comuna_calculada'] == 13],
        'c14':    df[df['comuna_calculada'] == 14],
        'zona1a': df[df['comuna_calculada'] == 1.5],
        'resto':  df[~df['comuna_calculada'].isin(ZONAS_PRIORIZADAS)],
        'total':  df,
    }
    all_cierres_data = {k: compute_cierres_breakdown_weekly(v) for k, v in _zona_dfs.items()}

    print(f" Generando HTML Interactivo...")
    
    with open(TEMPLATE_HTML_PATH, 'r', encoding='utf-8') as f:
        html = f.read()

    # --- CAMBIOS DE NOMBRE (Refinamiento Final) ---
    html = html.replace("Red BAP", "Red de Atencin")
    html = html.replace("BAP Personas", "Red de Atencin")

    # --- LOGO (Base64) ---
    import base64
    logo_b64 = ""
    logo_path = "logoba-removebg-preview.png"
    
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as image_file:
            logo_b64 = base64.b64encode(image_file.read()).decode('utf-8')
            img_tag = f'<img src="data:image/png;base64,{logo_b64}" alt="BA Logo" class="h-16 w-auto object-contain" />'
    else:
        # Fallback si no encuentra la imagen
        img_tag = '<span class="text-white font-bold text-xl">BA</span>'

    # --- NUEVO HEADER (Diseo Visual) ---
    # Reemplazamos todo el bloque <header>...</header> del template original
    new_header = f'''
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
                          <div>Actualizado: 01/01/2000 00:00</div>
                          <div class="text-gray-600">Semana: 01 Jan</div>
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
    '''
    
    html = re.sub(r'<header.*?</header>', new_header, html, flags=re.DOTALL)


    # --- INYECCIONES ---
    
    # 1. CDN Compatibles (Chart.js 3.9.1 + Datalabels 2.2.0)
    head_libs = '''
    <script src="https://cdn.jsdelivr.net/npm/chart.js@3.9.1/dist/chart.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0"></script>
    '''
    
    html = re.sub(r'<script src=".*?chart\.js.*?"></script>', '', html)
    html = re.sub(r'<script src=".*?chartjs-plugin-datalabels.*?"></script>', '', html)
    html = html.replace('<head>', f'<head>{head_libs}')

    # 2. Contenedores de Tablas
    def build_container_html(container_id, title, default_key):
        zona_opts = [
            ("c2",      "Comuna 2"),
            ("c13",     "Comuna 13"),
            ("c14",     "Comuna 14"),
            ("zona1a",  "Zona 1A"),
            ("resto",   "Resto de la ciudad"),
            ("total",   "Total Ciudad"),
        ]
        opts = ""
        for key, label in zona_opts:
            sel = "selected" if key == default_key else ""
            opts += f'<option value="{key}" {sel}>{label}</option>'

        return f'''
            <div class="bg-white rounded-xl shadow-lg overflow-hidden border border-gray-200">
                <div class="bg-teal-600 p-4 text-white font-bold text-lg flex justify-between items-center">
                    <span>{title}</span>
                    <select class="text-xs text-gray-800 p-1 rounded cursor-pointer focus:outline-none" 
                            onchange="renderTable('{container_id}', this.value)">
                        {opts}
                    </select>
                </div>
                <div class="overflow-x-auto" id="{container_id}"></div>
            </div>
        '''

    new_section_content = f'''
        <section class="grid grid-cols-1 lg:grid-cols-2 gap-8">
            {build_container_html('table1', 'Panel Izquierdo', 'c2')}
            {build_container_html('table2', 'Panel Derecho', 'total')}
        </section>
    '''
    
    html = re.sub(
        r'<!-- SECCION 1: TABLAS -->\s*<section.*?>(.*?)</section>', 
        f'<!-- SECCION 1: TABLAS -->\n{new_section_content}', 
        html, 
        flags=re.DOTALL
    )

    # 3. Grficos Duales (vinculados a Panel Izquierdo y Panel Derecho)
    def build_chart_section(panel_id, default_label):
        return f'''
        <section class="bg-white rounded-xl shadow-lg p-6 border border-gray-200">
            <h2 id="chartTitle{panel_id}" class="text-xl font-bold text-gray-800 mb-6 border-b pb-2">Evolucin Semanal de DNI\'s - {default_label}</h2>
            <div class="relative h-96 w-full">
                <canvas id="chart{panel_id}"></canvas>
            </div>
        </section>
        '''

    charts_html = f'''
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {build_chart_section(1, "Comuna 2")}
        {build_chart_section(2, "Total Ciudad")}
    </div>
    '''

    # Reemplazamos la antigua seccion de graficos (que era una sola <section>)
    html = re.sub(
        r'<!-- SECCION 2: GRAFICOS -->\s*<section.*?</section>',
        f'<!-- SECCION 2: GRAFICOS -->\n{charts_html}',
        html,
        flags=re.DOTALL
    )

    # 4. Lgica JS
    json_all = json.dumps(all_data)
    json_all_charts = json.dumps(all_chart_data)
    json_all_cierres = json.dumps(all_cierres_data)

    js_logic = f'''
    <script>
        // DATOS GLOBALES
        const allComunaData = {json_all};
        const allChartData = {json_all_charts};
        const allCierresData = {json_all_cierres};
        const chartInstances = {{}};

        // ETIQUETA LEGIBLE POR CLAVE
        function getComLabel(key) {{
            if (key === 'total') return 'Total Ciudad';
            if (key === 'c14') return 'Comuna 14';
            if (key === 'c13') return 'Comuna 13';
            if (key === 'zona1a') return 'Zona 1A';
            if (key === 'resto') return 'Resto de la ciudad';
            if (key === 'c2') return 'Comuna 2';
            return 'Comuna ' + key.replace('c', '');
        }}

        // RENDER TABLA
        function renderTable(containerId, key) {{
            const data = allComunaData[key];
            if (!data) return;
            const container = document.getElementById(containerId);

            let ths = '<th class="p-3 text-left">Indicadores</th><th class="p-3 w-20 bg-teal-800">Lnea Base</th>';
            data.weeks.forEach(w => ths += `<th class="p-3 w-24">${{w}}</th>`);

            let trs = '';
            data.rows.forEach((r, idx) => {{
                let tds = '';
                r.vals.forEach(v => tds += `<td class="p-3 text-gray-800">${{v}}</td>`);
                trs += `
                    <tr class="hover:bg-yellow-50 transition-colors">
                        <td class="p-3 text-left font-semibold text-gray-700 bg-gray-50 sticky left-0">${{r.label}}</td>
                        <td class="p-3 font-bold text-gray-600 bg-gray-100 border-r border-gray-300">${{r.base}}</td>
                        ${{tds}}
                    </tr>`;
            }});

            container.innerHTML = `<table class="w-full text-sm text-center"><thead><tr class="bg-teal-700 text-white">${{ths}}</tr></thead><tbody class="divide-y divide-gray-200">${{trs}}</tbody></table>`;

            // Actualizar grfico vinculado al panel
            const panelId = containerId === 'table1' ? 1 : 2;
            updateChart(panelId, key);
        }}

        // ACTUALIZAR GRFICO
        function updateChart(panelId, key) {{
            const data = allChartData[key];
            if (!data) return;

            // Actualizar ttulo
            const titleEl = document.getElementById('chartTitle' + panelId);
            if (titleEl) titleEl.textContent = "Evolucin Semanal de DNI's - " + getComLabel(key);

            // Destruir instancia anterior si existe
            if (chartInstances[panelId]) {{
                chartInstances[panelId].destroy();
                delete chartInstances[panelId];
            }}

            if (typeof ChartDataLabels !== 'undefined') {{
                Chart.register(ChartDataLabels);
            }}

            const ctx = document.getElementById('chart' + panelId).getContext('2d');
            chartInstances[panelId] = new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        x: {{ stacked: true, grid: {{ display: false }} }},
                        y: {{ stacked: true, beginAtZero: true }}
                    }},
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{ mode: 'index', intersect: false }},
                        datalabels: {{
                            color: 'white',
                            font: {{ weight: 'bold', size: 10 }},
                            formatter: (value) => value > 0 ? value : ''
                        }}
                    }}
                }},
                plugins: [{{
                    id: 'totalLabels',
                    afterDatasetsDraw: (chart) => {{
                        const ctx = chart.ctx;
                        chart.data.labels.forEach((label, index) => {{
                            let total = 0;
                            chart.data.datasets.forEach(ds => total += ds.data[index]);
                            if (total > 0) {{
                                const meta = chart.getDatasetMeta(chart.data.datasets.length - 1);
                                const x = meta.data[index].x;
                                const y = meta.data[index].y;
                                ctx.fillStyle = 'black';
                                ctx.font = 'bold 11px Inter';
                                ctx.textAlign = 'center';
                                ctx.fillText(total, x, y - 5);
                            }}
                        }});
                    }}
                }}]
            }});
        }}

        // GRAFICO CIERRES
        let cierresChartInstance = null;

        function renderCierresChart(key) {{
            const data = allCierresData[key];
            if (!data) return;

            const titleEl = document.getElementById('cierresChartTitle');
            if (titleEl) titleEl.textContent = 'Cierres por tipo (Se contacta) - ' + getComLabel(key);

            if (cierresChartInstance) {{
                cierresChartInstance.destroy();
                cierresChartInstance = null;
            }}

            if (typeof ChartDataLabels !== 'undefined') {{
                Chart.register(ChartDataLabels);
            }}

            const ctx = document.getElementById('cierresChart').getContext('2d');
            cierresChartInstance = new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        x: {{ stacked: true, grid: {{ display: false }} }},
                        y: {{ stacked: true, beginAtZero: true }}
                    }},
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{ mode: 'index', intersect: false }},
                        datalabels: {{
                            color: 'white',
                            font: {{ weight: 'bold', size: 9 }},
                            formatter: (value) => value > 0 ? value : ''
                        }}
                    }}
                }},
                plugins: [{{
                    id: 'cierresTotals',
                    afterDatasetsDraw: (chart) => {{
                        const ctx = chart.ctx;
                        chart.data.labels.forEach((label, index) => {{
                            let total = 0;
                            chart.data.datasets.forEach(ds => total += ds.data[index]);
                            if (total > 0) {{
                                const meta = chart.getDatasetMeta(chart.data.datasets.length - 1);
                                const x = meta.data[index].x;
                                const y = meta.data[index].y;
                                ctx.fillStyle = 'black';
                                ctx.font = 'bold 11px Inter';
                                ctx.textAlign = 'center';
                                ctx.fillText(total, x, y - 5);
                            }}
                        }});
                    }}
                }}]
            }});
        }}

        // INICIALIZACIÓN
        document.addEventListener('DOMContentLoaded', function() {{
            renderTable('table1', 'c2');
            renderTable('table2', 'total');
            renderCierresChart('total');
        }});

    </script>
    '''

    # Usamos replace del bloque script final
    html = re.sub(r'<script>\s*// Datos inyectados desde Python.*?</script>', js_logic, html, flags=re.DOTALL)

    # SECCION 3: grafico de cierres por tipo PPT
    zona_opts_cierres = ''.join(
        f'<option value="{k}" {"selected" if k == "total" else ""}>{l}</option>'
        for k, l in [("c2","Comuna 2"),("c13","Comuna 13"),("c14","Comuna 14"),
                     ("zona1a","Zona 1A"),("resto","Resto de la ciudad"),("total","Total Ciudad")]
    )
    seccion3_html = f'''
        <!-- SECCION 3: CIERRES -->
        <section class="bg-white rounded-xl shadow-lg p-6 border border-gray-200">
            <div class="flex justify-between items-center mb-6 border-b pb-2">
                <h2 id="cierresChartTitle" class="text-xl font-bold text-gray-800">
                    Cierres por tipo (Se contacta) - Total Ciudad
                </h2>
                <select class="text-xs text-gray-800 border border-gray-300 p-1 rounded cursor-pointer focus:outline-none"
                        onchange="renderCierresChart(this.value)">
                    {zona_opts_cierres}
                </select>
            </div>
            <div class="relative h-96 w-full">
                <canvas id="cierresChart"></canvas>
            </div>
        </section>
    '''
    html = html.replace('</main>', seccion3_html + '\n    </main>')

    # Info Header
    html = re.sub(r'Actualizado: .*?</div>', f'Actualizado: {last_update}</div>', html)
    if chart_json_c2['labels']:
        last_week_label = chart_json_c2['labels'][-1]
        html = re.sub(r'Semana: .*?</div>', f'Semana: {last_week_label}</div>', html)

    with open(OUTPUT_HTML_PATH, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print("[OK] Dashboard Interactivo generado.")

if __name__ == '__main__':
    main()
