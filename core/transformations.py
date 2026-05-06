"""
Data transformation and cleaning utilities.

This module contains all the cleaning functions, regex patterns, and categorization
logic used in the data processing pipeline.
"""

import pandas as pd
import numpy as np
import re
import unicodedata
import unidecode
from rapidfuzz import process, fuzz


# ==========================================
# TEXT CLEANING FUNCTIONS
# ==========================================

def limpiar_texto(nombre):
    """Limpia y normaliza nombres y apellidos."""
    if pd.isna(nombre): 
        return None
    nombre = str(nombre).upper()
    nombre = ''.join(c for c in unicodedata.normalize('NFD', nombre) if unicodedata.category(c) != 'Mn')
    nombre = re.sub(r'[-.,]', ' ', nombre)
    nombre = re.sub(r'[^A-Z ]', '', nombre)
    nombre = re.sub(r'\s+', ' ', nombre).strip()
    return nombre if nombre else None


def limpiar_texto_cierre(s):
    """Normaliza texto de cierre para matching: lowercase, sin acentos, sin guiones."""
    if pd.isna(s): 
        return ""
    s = str(s).lower().strip()
    s = unidecode.unidecode(s)
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# ==========================================
# DNI CLEANING AND CATEGORIZATION
# ==========================================

# Regex patterns for DNI classification
PATRON_EXTRANJERO = re.compile(
    r'(extranjero|paraguay|venezol|colombian|uruguay|brasil|chilen|peruano|mexican|'
    r'espaol|dominican|dominicana|pasaporte|c\\.?d\\.?[ie]:?|rnm|cedula|ciudadano\\s+extranjero)',
    flags=re.IGNORECASE
)
PATRON_NO_BRINDO_GENERICOS = re.compile(
    r'(no\\s*brind|no\\s*bri[nm]d|no\\s*aporta|no\\s*aporto|no\\s*indica|no\\s*sabe|'
    r'no\\s*recuerda|no\\s*recuerd|no\\s*tiene|nunca\\s*tuvo|sin\\s*dni|sin\\s*dato|'
    r'sin\\s*inform|ilegible|invisible|no\\s*visible|exhib|no\\s*lo\\s*sabe|menor\\s*de\\s*edad)',
    flags=re.IGNORECASE
)
PATRON_NO_BRINDO_SIMBOLOS = re.compile(r'^[xX\\*\\-\\.]+$', flags=re.IGNORECASE)
PATRON_LETRAS_CORTAS = re.compile(r'^[A-Za-z]{1,3}$')
PATRON_SOLO_LETRAS = re.compile(r'^[A-Za-z]+$')


def limpiar_y_categorizar_dni_v3(df, columna_original, columna_salida=None, crear_motivo=True):
    """Limpia y categoriza DNI: numérico válido, NO BRINDO/NO VISIBLE, CONTACTO EXTRANJERO."""
    if columna_salida is None: 
        columna_salida = columna_original
    motivo_col = f"{columna_salida}_motivo" if crear_motivo else None

    def procesar_valor(v):
        if pd.isna(v): 
            return ('NO BRINDO/NO VISIBLE', 'nan')
        s = str(v).strip()
        if s == '': 
            return ('NO BRINDO/NO VISIBLE', 'empty')
        s_lower = s.lower()
        if PATRON_NO_BRINDO_GENERICOS.search(s_lower): 
            return ('NO BRINDO/NO VISIBLE', 'patron_no_brindo_genericos')
        if PATRON_NO_BRINDO_SIMBOLOS.match(s) or PATRON_LETRAS_CORTAS.match(s): 
            return ('NO BRINDO/NO VISIBLE', 'simbolos_o_letras_cortas')
        if PATRON_SOLO_LETRAS.match(s) and len(set(s_lower)) <= 2: 
            return ('NO BRINDO/NO VISIBLE', 'solo_letras_repetidas')
        if PATRON_EXTRANJERO.search(s_lower): 
            return ('CONTACTO EXTRANJERO', 'patron_extranjero')
        
        # Limpieza robusta de dgitos
        digits = re.sub(r'\D', '', s)
        
        if 6 <= len(digits) <= 10: 
            try:
                return (int(digits), 'dni_valido')
            except ValueError:
                return ('NO BRINDO/NO VISIBLE', 'error_conversion')
                
        if len(digits) < 6 or re.search(r'[A-Za-z]', s_lower): 
            return ('NO BRINDO/NO VISIBLE', 'texto_o_corto')
        return ('NO BRINDO/NO VISIBLE', 'resto_no_brindo')

    print(f" Procesando DNI: {columna_original}...")
    resultados = df[columna_original].apply(procesar_valor)
    df[columna_salida] = resultados.apply(lambda x: x[0])
    if crear_motivo:
        df[motivo_col] = resultados.apply(lambda x: x[1])
    return df


# ==========================================
# CATEGORIZATION OF INTERVENTION OUTCOMES
# ==========================================

# Canonical new values — single source of truth (vigente desde 01/05/2026)
# DERIVACION A RED, DERIVACION AREA CNNyA-102 y POSITIVO eliminados del sistema Soflex.
CATEGORIAS_NUEVAS = [
    "01. Traslado efectivo a CIS",
    "02. Traslado efectivo a DIPA",
    "03. Traslado efectivo a Micro",
    "05. Traslado efectivo a lugar de origen",
    "06. Acepta CIS pero no hay vacante",
    "07. Se realiza entrevista y se retira del lugar",
    "08. No se realiza entrevista y se retira del lugar",
    "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "12. Derivación a SAME por deterioro físico visible",
    "13. Derivación a SAME por salud mental",
    "14. Derivación a Seguridad",
    "15. Derivación a Ordenamiento Urbano",
    "16. No se observan personas y hay pertenencias",
    "17. No se observan personas ni pertenencias",
    "18. Mendicidad",
    "19. Sin cubrir",
    "20. Desestimado",
]

# Nivel de contacto por categoria canónica
NIVEL_POR_CIERRE = {
    "01. Traslado efectivo a CIS":                                   "Se contacta",
    "02. Traslado efectivo a DIPA":                                  "Se contacta",
    "03. Traslado efectivo a Micro":                                 "Se contacta",
    "05. Traslado efectivo a lugar de origen":                       "Se contacta",
    "06. Acepta CIS pero no hay vacante":                            "Se contacta",
    "07. Se realiza entrevista y se retira del lugar":               "Se contacta",
    "08. No se realiza entrevista y se retira del lugar":            "Se contacta",
    "09. Derivación al equipo de Umbral Cero de Primer Abordaje":   "Se contacta",
    "12. Derivación a SAME por deterioro físico visible":            "Se contacta",
    "13. Derivación a SAME por salud mental":                        "Se contacta",
    "14. Derivación a Seguridad":                                    "Se contacta",
    "15. Derivación a Ordenamiento Urbano":                          "Se contacta",
    "18. Mendicidad":                                                "Se contacta",
    "16. No se observan personas y hay pertenencias":                "No se contacta",
    "17. No se observan personas ni pertenencias":                   "No se contacta",
    "19. Sin cubrir":                                                "Sin cubrir",
    "20. Desestimado":                                               "Desestimado",
}

# Bucket de reporte por categoria canónica — etiquetas PPT
BUCKET_POR_CIERRE = {
    "01. Traslado efectivo a CIS":                                   "SE DERIVA",
    "02. Traslado efectivo a DIPA":                                  "SE DERIVA",
    "03. Traslado efectivo a Micro":                                 "SE DERIVA",
    "05. Traslado efectivo a lugar de origen":                       "SE DERIVA",
    "06. Acepta CIS pero no hay vacante":                            "ACEPTA CIS SIN VACANTE",
    "07. Se realiza entrevista y se retira del lugar":               "SE RETIRA",
    "08. No se realiza entrevista y se retira del lugar":            "SE RETIRA",
    "09. Derivación al equipo de Umbral Cero de Primer Abordaje":   "SE DERIVA",
    "12. Derivación a SAME por deterioro físico visible":            "CASOS DE SALUD MENTAL",
    "13. Derivación a SAME por salud mental":                        "CASOS DE SALUD MENTAL",
    "14. Derivación a Seguridad":                                    "SE DERIVA",
    "15. Derivación a Ordenamiento Urbano":                          "ESPACIO PUBLICO",
    "16. No se observan personas y hay pertenencias":                "NO SE CONTACTA",
    "17. No se observan personas ni pertenencias":                   "NO SE CONTACTA",
    "18. Mendicidad":                                                "MENDICIDAD",
    "19. Sin cubrir":                                                "SIN CUBRIR",
    "20. Desestimado":                                               "DESESTIMADO",
}

# Sets para breakdown de entrevista — solo cierres con interacción directa que solicita DNI
REALIZA_ENTREVISTA_CATS = {
    "01. Traslado efectivo a CIS",
    "02. Traslado efectivo a DIPA",
    "03. Traslado efectivo a Micro",
    "05. Traslado efectivo a lugar de origen",
    "06. Acepta CIS pero no hay vacante",
    "07. Se realiza entrevista y se retira del lugar",
    "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "18. Mendicidad",
}

DERIVADO_CATS = {
    "01. Traslado efectivo a CIS",
    "02. Traslado efectivo a DIPA",
    "03. Traslado efectivo a Micro",
    "05. Traslado efectivo a lugar de origen",
    "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "12. Derivación a SAME por deterioro físico visible",
    "13. Derivación a SAME por salud mental",
    "14. Derivación a Seguridad",
    "15. Derivación a Ordenamiento Urbano",
}

# Sets para obtener_niveles() — columnas contacto / brinda_datos
SE_CONTACTA_BRINDA_DATOS = {
    "01. Traslado efectivo a CIS",
    "02. Traslado efectivo a DIPA",
    "03. Traslado efectivo a Micro",
    "05. Traslado efectivo a lugar de origen",
    "06. Acepta CIS pero no hay vacante",
    "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "12. Derivación a SAME por deterioro físico visible",
    "13. Derivación a SAME por salud mental",
    "14. Derivación a Seguridad",
    "15. Derivación a Ordenamiento Urbano",
    "18. Mendicidad",
}
SE_CONTACTA_NO_BRINDA_DATOS = {
    "07. Se realiza entrevista y se retira del lugar",
    "08. No se realiza entrevista y se retira del lugar",
}
NO_CONTACTA_SET = {
    "16. No se observan personas y hay pertenencias",
    "17. No se observan personas ni pertenencias",
}

# Mapa viejo→nuevo (Tier 0): clave = limpiar_texto_cierre(valor_viejo_o_nuevo)
# Cubre tanto el esquema legacy como el nuevo para que ambos resuelvan al canónico
MAPEO_VIEJO_A_NUEVO = {
    # --- Legacy (prefijo NN-) → nuevo ---
    "01 traslado efectivo a cis":                          "01. Traslado efectivo a CIS",
    "02 acepta cis pero no hay vacante":                   "06. Acepta CIS pero no hay vacante",
    "03 se activa protocolo de salud mental":              "13. Derivación a SAME por salud mental",
    "05 derivacion a same":                                "12. Derivación a SAME por deterioro físico visible",
    "07 rechaza entrevista y se retira del lugar":         "07. Se realiza entrevista y se retira del lugar",
    "10 derivacion a espacio publico":                     "15. Derivación a Ordenamiento Urbano",
    "11 no se contacta y se observan pertenencias":        "16. No se observan personas y hay pertenencias",
    "12 no se contacta y no se observan pertenencias":     "17. No se observan personas ni pertenencias",
    "13 mendicidad menores de edad":                       "18. Mendicidad",
    "15 sin cubrir":                                       "19. Sin cubrir",
    "16 desestimado cartas 911 u otras areas":             "20. Desestimado",
    # --- Nuevo (prefijo NN.) → canónico (self-map para igualdad exacta) ---
    "01 traslado efectivo a cis 2":                        "01. Traslado efectivo a CIS",  # edge case
    "02 traslado efectivo a dipa":                         "02. Traslado efectivo a DIPA",
    "03 traslado efectivo a micro":                        "03. Traslado efectivo a Micro",
    "05 traslado efectivo a lugar de origen":              "05. Traslado efectivo a lugar de origen",
    "06 acepta cis pero no hay vacante":                   "06. Acepta CIS pero no hay vacante",
    "07 se realiza entrevista y se retira del lugar":      "07. Se realiza entrevista y se retira del lugar",
    "08 no se realiza entrevista y se retira del lugar":   "08. No se realiza entrevista y se retira del lugar",
    "09 derivacion al equipo de umbral cero de primer abordaje": "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "12 derivacion a same por deterioro fisico visible":   "12. Derivación a SAME por deterioro físico visible",
    "13 derivacion a same por salud mental":               "13. Derivación a SAME por salud mental",
    "14 derivacion a seguridad":                           "14. Derivación a Seguridad",
    "15 derivacion a ordenamiento urbano":                 "15. Derivación a Ordenamiento Urbano",
    "16 no se observan personas y hay pertenencias":       "16. No se observan personas y hay pertenencias",
    "17 no se observan personas ni pertenencias":          "17. No se observan personas ni pertenencias",
    "18 mendicidad":                                       "18. Mendicidad",
    "19 sin cubrir":                                       "19. Sin cubrir",
    "20 desestimado":                                      "20. Desestimado",
    "derivacion a red":                                    "error de soflex",
    "derivacion area cnnya 102":                           "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "11 derivacion area cnnya 102":                        "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "positivo":                                            "sin_match",
    # --- Legacy sin mapeo previo → proxy canónico ---
    "06 se realiza entrevista":                            "08. No se realiza entrevista y se retira del lugar",
    "09 rechaza entrevista y se queda en el lugar":        "08. No se realiza entrevista y se retira del lugar",
    "04 traslado acompanamiento a otros efectores":        "05. Traslado efectivo a lugar de origen",
    "04 traslado efectivo 690":                            "01. Traslado efectivo a CIS",
    "08 imposibilidad de abordaje por consumo":            "08. No se realiza entrevista y se retira del lugar",
    "14 no se encuentra en situacion de calle":            "17. No se observan personas ni pertenencias",
    "negativo":                                            "20. Desestimado",
    "derivacion a ep":                                     "15. Derivación a Ordenamiento Urbano",
}

# Exact match para códigos compuestos del sistema DIPA (Tier 1)
PATRONES_EXACTOS = {
    "17 dipa derivacion a cis":                                                                             "01. Traslado efectivo a CIS",
    "01 positivo traslado a cis hogar 08 positivo derivacion a sas cud cp identidad etc":                  "01. Traslado efectivo a CIS",
    "10 se contacta pero rechaza pp por desconocimiento voluntad etc":                                      "08. No se realiza entrevista y se retira del lugar",
    "21 asesoramiento sobre programas":                                                                     "07. Se realiza entrevista y se retira del lugar",
    "16 dipa entrega de insumos servicios 21 asesoramiento sobre programas":                               "sin_match",
    "16 dipa entrega de insumos servicios 7 positivo entrega de insumos":                                  "sin_match",
    "21 asesoramiento sobre programas 16 dipa entrega de insumos servicios":                               "sin_match",
    "7 positivo entrega de insumos 16 dipa entrega de insumos servicios":                                  "sin_match",
    "7 positivo entrega de insumos 21 asesoramiento sobre programas":                                      "sin_match",
    "21 asesoramiento sobre programas 7 positivo entrega de insumos":                                      "sin_match",
    "16 dipa entrega de insumos servicios 21 asesoramiento sobre programas 7 positivo entrega de insumos": "sin_match",
    "11 se contacta pero rechaza pp por disconformidad egresado":                                          "08. No se realiza entrevista y se retira del lugar",
    "10 se contacta pero rechaza pp por desconocimiento voluntad etc 11 se contacta pero rechaza pp por disconformidad egresado": "08. No se realiza entrevista y se retira del lugar",
    "9 se contacta pero rechaza entrevista 21 asesoramiento sobre programas":                              "08. No se realiza entrevista y se retira del lugar",
    "9 se contacta pero rechaza entrevista 24 persona abandona el lugar por intervencion ep policia":      "08. No se realiza entrevista y se retira del lugar",
    "21 asesoramiento sobre programas 10 se contacta pero rechaza pp por desconocimiento voluntad etc":    "08. No se realiza entrevista y se retira del lugar",
    "10 se contacta pero rechaza pp por desconocimiento voluntad etc 21 asesoramiento sobre programas":    "08. No se realiza entrevista y se retira del lugar",
    "21 asesoramiento sobre programas 9 se contacta pero rechaza entrevista":                              "08. No se realiza entrevista y se retira del lugar",
    "10 se contacta pero rechaza pp por desconocimiento voluntad etc 9 se contacta pero rechaza entrevista": "08. No se realiza entrevista y se retira del lugar",
    "asesoramiento sobre programas rechan entrevista se quedan en el lugar":                               "08. No se realiza entrevista y se retira del lugar",
}

# Substring match (Tier 2) — ordered from most specific to least
PATRONES_PERSONALIZADOS = {
    "deterioro fisico":                "12. Derivación a SAME por deterioro físico visible",
    "salud mental":                    "13. Derivación a SAME por salud mental",
    "umbral cero":                     "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "ordenamiento urbano":             "15. Derivación a Ordenamiento Urbano",
    "lugar de origen":                 "05. Traslado efectivo a lugar de origen",
    "traslado a micro":                "03. Traslado efectivo a Micro",
    "traslado efectivo a micro":       "03. Traslado efectivo a Micro",
    "traslado a dipa":                 "02. Traslado efectivo a DIPA",
    "traslado efectivo a dipa":        "02. Traslado efectivo a DIPA",
    "derivacion a cis":                "01. Traslado efectivo a CIS",
    "traslado a cis":                  "01. Traslado efectivo a CIS",
    "traslado efectivo a cis":         "01. Traslado efectivo a CIS",
    "acepta cis":                      "06. Acepta CIS pero no hay vacante",
    "protocolo de salud mental":       "13. Derivación a SAME por salud mental",
    " same":                           "12. Derivación a SAME por deterioro físico visible",
    "se realiza entrevista y se retira": "07. Se realiza entrevista y se retira del lugar",
    "no se realiza entrevista":        "08. No se realiza entrevista y se retira del lugar",
    "rechaza entrevista y se retira":  "08. No se realiza entrevista y se retira del lugar",
    "rechaza entrevista":              "08. No se realiza entrevista y se retira del lugar",
    "imposibilidad de abordaje":       "08. No se realiza entrevista y se retira del lugar",
    "espacio publico":                 "15. Derivación a Ordenamiento Urbano",
    "mendicidad":                      "18. Mendicidad",
    "sin cubrir":                      "19. Sin cubrir",
    "desestimado":                     "20. Desestimado",
    "derivacion a red":                "error de soflex",
    "derivacion area cnnya":           "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "cnnya":                           "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "cnnva":                           "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "nnnya":                           "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "nnya 102":                        "09. Derivación al equipo de Umbral Cero de Primer Abordaje",
    "seguridad":                       "14. Derivación a Seguridad",
}

# Canonical list in cleaned form for Tier 3 fuzzy matching
_CATS_LIMPIAS = {limpiar_texto_cierre(c): c for c in CATEGORIAS_NUEVAS}
_CATS_LIMPIAS_LIST = list(_CATS_LIMPIAS.keys())


def mapear_categoria_con_reglas(texto):
    """Tier 0: MAPEO_VIEJO_A_NUEVO → Tier 1: PATRONES_EXACTOS → Tier 2: substring → Tier 3: fuzzy ≥80."""
    if pd.isna(texto):
        return "sin_match"
    texto = str(texto).strip()
    if texto == "":
        return "sin_match"

    # Tier 0: direct value map (old schema → new, new schema → new)
    if texto in MAPEO_VIEJO_A_NUEVO:
        return MAPEO_VIEJO_A_NUEVO[texto]

    # Tier 1: exact match for DIPA combo codes
    if texto in PATRONES_EXACTOS:
        return PATRONES_EXACTOS[texto]

    # Tier 2: substring match
    for patron, categoria in PATRONES_PERSONALIZADOS.items():
        if patron in texto:
            return categoria

    # Tier 3: fuzzy match against cleaned canonical list
    mejor_clean, score, _ = process.extractOne(texto, _CATS_LIMPIAS_LIST, scorer=fuzz.WRatio)
    return _CATS_LIMPIAS[mejor_clean] if score >= 80 else "sin_match"


def obtener_niveles(cat):
    """Devuelve (nivel_contacto_texto, brinda_datos_texto) para columnas ETL."""
    if cat in SE_CONTACTA_BRINDA_DATOS:
        return "Contacta", "Brinda datos"
    if cat in SE_CONTACTA_NO_BRINDA_DATOS:
        return "Contacta", "No brinda datos"
    if cat in NO_CONTACTA_SET:
        return "No se contacta", ""
    if cat == "19. Sin cubrir":
        return "Sin cubrir", ""
    if cat == "20. Desestimado":
        return "Desestimado", ""
    return "Derivaciones/seguimientos", ""


# ==========================================
# NIVEL DE CONTACTO (columna de alto nivel)
# ==========================================

def obtener_nivel_contacto(cat):
    """Mapea categoria_final al nivel de contacto de alto nivel via NIVEL_POR_CIERRE."""
    if pd.isna(cat):
        return "Sin dato"
    cat_str = str(cat).strip()
    if cat_str in ("", "sin_match"):
        return "Sin dato"
    return NIVEL_POR_CIERRE.get(cat_str, "Sin dato")
