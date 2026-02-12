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
    """
    Limpia y normaliza nombres y apellidos.
    
    Args:
        nombre: Input text to clean
        
    Returns:
        str or None: Cleaned text in uppercase, or None if invalid
    """
    if pd.isna(nombre): 
        return None
    nombre = str(nombre).upper()
    nombre = ''.join(c for c in unicodedata.normalize('NFD', nombre) if unicodedata.category(c) != 'Mn')
    nombre = re.sub(r'[-.,]', ' ', nombre)
    nombre = re.sub(r'[^A-Z ]', '', nombre)
    nombre = re.sub(r'\\s+', ' ', nombre).strip()
    return nombre if nombre else None


def limpiar_texto_cierre(s):
    """
    Limpia texto de categorías de cierre para matching.
    
    Args:
        s: Input text to clean
        
    Returns:
        str: Cleaned text in lowercase
    """
    if pd.isna(s): 
        return ""
    s = str(s).lower().strip()
    s = unidecode.unidecode(s)
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"[^a-z0-9áéíóúüñ\\s]", " ", s)
    s = re.sub(r"\\s+", " ", s)
    return s.strip()


# ==========================================
# DNI CLEANING AND CATEGORIZATION
# ==========================================

# Regex patterns for DNI classification
PATRON_EXTRANJERO = re.compile(
    r'(extranjero|paraguay|venezol|colombian|uruguay|brasil|chilen|peruano|mexican|'
    r'español|dominican|dominicana|pasaporte|c\\.?d\\.?[ie]:?|rnm|cedula|ciudadano\\s+extranjero)', 
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
    """
    Limpia y categoriza valores de DNI según reglas específicas.
    
    Args:
        df (pd.DataFrame): DataFrame to process
        columna_original (str): Name of the column with DNI values
        columna_salida (str, optional): Name of the output column. Defaults to columna_original
        crear_motivo (bool): Whether to create a column with categorization reason
        
    Returns:
        pd.DataFrame: DataFrame with cleaned DNI column
    """
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
        
        # Limpieza robusta de dígitos
        digits = re.sub(r'\D', '', s)
        
        if 6 <= len(digits) <= 10: 
            try:
                return (int(digits), 'dni_valido')
            except ValueError:
                return ('NO BRINDO/NO VISIBLE', 'error_conversion')
                
        if len(digits) < 6 or re.search(r'[A-Za-z]', s_lower): 
            return ('NO BRINDO/NO VISIBLE', 'texto_o_corto')
        return ('NO BRINDO/NO VISIBLE', 'resto_no_brindo')

    print(f"⚙️ Procesando DNI: {columna_original}...")
    resultados = df[columna_original].apply(procesar_valor)
    df[columna_salida] = resultados.apply(lambda x: x[0])
    if crear_motivo: 
        df[motivo_col] = resultados.apply(lambda x: x[1])
    return df


# ==========================================
# CATEGORIZATION OF INTERVENTION OUTCOMES
# ==========================================

# Category lists
CATEGORIAS_BRINDA_DATOS = [
    "traslado efectivo a cis", 
    "acepta cis pero no hay vacante", 
    "se activa protocolo de salud mental", 
    "derivacion a same", 
    "traslado/acompanamiento a otros efectores", 
    "mendicidad (menores de edad)"
]
CATEGORIAS_NO_BRINDA_DATOS = [
    "se realiza entrevista", 
    "rechaza entrevista y se retira del lugar", 
    "imposibilidad de abordaje por consumo", 
    "rechaza entrevista y se queda en el lugar", 
    "derivacion a espacio publico", 
    "no se encuentra en situacion de calle"
]
CATEGORIAS_NO_CONTACTA = [
    "no se contacta y se observan pertenencias", 
    "no se contacta y no se observan pertenencias", 
    "sin cubrir", 
    "desestimado (cartas 911 u otras areas)"
]
CATEGORIAS_TODAS = CATEGORIAS_BRINDA_DATOS + CATEGORIAS_NO_BRINDA_DATOS + CATEGORIAS_NO_CONTACTA

# Exact match patterns (higher priority)
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

# Substring match patterns (medium priority)
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
    """
    Maps intervention outcome text to standard categories using a 3-tier approach:
    1. Exact match
    2. Substring match
    3. Fuzzy match
    
    Args:
        texto (str): Cleaned intervention outcome text
        
    Returns:
        str: Mapped category or "sin_match" if no match found
    """
    # Tier 1: Exact match
    if texto in PATRONES_EXACTOS: 
        return PATRONES_EXACTOS[texto]
    
    # Tier 2: Substring match
    for patron, categoria in PATRONES_PERSONALIZADOS.items():
        if patron in texto: 
            return categoria
    
    # Tier 3: Fuzzy match
    mejor_match, score, _ = process.extractOne(texto, CATEGORIAS_TODAS, scorer=fuzz.WRatio)
    return mejor_match if score >= 80 else "sin_match"


def obtener_niveles(cat):
    """
    Determines contact and data provision levels based on category.
    
    Args:
        cat (str): Category name
        
    Returns:
        tuple: (contact_level, data_level)
    """
    if cat in CATEGORIAS_BRINDA_DATOS: 
        return "Contacta", "Brinda datos"
    elif cat in CATEGORIAS_NO_BRINDA_DATOS: 
        return "Contacta", "No brinda datos"
    elif cat in CATEGORIAS_NO_CONTACTA: 
        return "No se contacta", ""
    else: 
        return "Derivaciones/seguimientos", ""
