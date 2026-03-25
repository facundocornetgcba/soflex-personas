# Autom BAP Personas — Red de Atención

Sistema de automatización para el procesamiento, análisis y visualización de datos de intervenciones de la **Red de Atención** de la Ciudad de Buenos Aires.

El flujo principal es un **ETL incremental semanal** que toma el Excel más reciente subido a Drive, procesa solo los registros nuevos, los enriquece con geo y clasificación, y los vuelca a **Neon (PostgreSQL)** + backup Parquet en Drive.

---

## Estructura del proyecto

```
autom-bap-personas/
├── main.py                    # Orquestador: watermark → descarga Excel → ETL
├── data_processor.py          # ETL completo (geo, limpieza, categorización, append)
├── dashboard_generator.py     # Genera reporte_autom_bap.html con KPIs semanales
├── analisis_intervenciones.py # Análisis de categorías de resultado por mes/agencia
├── analisis_permanencia.py    # Análisis de recurrencia en zonas críticas
├── requirements.txt
└── core/
    ├── db_connections.py      # Conexión a Neon, watermark, stats
    ├── drive_manager.py       # Google Drive API (descarga/subida de archivos)
    └── transformations.py     # Limpieza de texto, DNI, y mapeo de categoria_final
```

---

## Uso

```bash
# Instalar dependencias
pip install -r requirements.txt

# Correr ETL semanal (requiere Excel nuevo en 01_insumos de Drive)
python main.py

# Generar dashboard
python dashboard_generator.py

# Análisis de intervenciones (genera HTML)
python analisis_intervenciones.py

# Análisis de permanencia
python analisis_permanencia.py
```

`credentials.json` debe estar en la raíz con:
- `DATABASE_URL`: connection string de Neon
- Credenciales de Service Account para Google Drive API

---

## Flujo del ETL (`main.py` → `data_processor.py`)

```
Excel en Drive (01_insumos)
        │
        ▼
Fase 0 ─ Leer Excel + filtrar por watermark (MAX Fecha Inicio en Neon)
        │
        ▼
Fase 0b─ Backup crudo incremental → Drive (2025_historico_v2.parquet)
        │
        ▼
Fase 1 ─ Geo: asignar comuna_calculada
         ├─ Palermo Norte (KMZ) → 14.5
         └─ SHP comunas CABA → 1-15
        │
        ▼
Fase 2 ─ Limpieza y categorización
         ├─ DNI: limpiar_y_categorizar_dni_v3()
         ├─ Nombres/apellidos: normalización de texto
         └─ categoria_final: ver sección abajo
        │
        ▼
Fase 3 ─ Tipo_Evolucion incremental (usa historial de Neon)
         ├─ Nuevo / Recurrente / Migratorio / No clasificable
         └─ "Nuevo repetido" si el mismo DNI aparece varias veces en la semana
        │
        ▼
Fase 3b─ Apariciones acumuladas por DNI (histórico Neon + lote nuevo)
        │
        ▼
Fase 4 ─ Append a Neon via COPY FROM STDIN (ultrarrápido, chunks de 50k)
        │
        ▼
Fase 5 ─ Actualizar backup limpio → Drive (2025_historico_limpio.parquet)
```

---

## Categorización de resultados (`categoria_final`)

### Columnas involucradas

| Columna origen | Descripción |
|---|---|
| `cierre_supervisor` | Texto de cierre ingresado por el supervisor (prioridad alta) |
| `resultado` | Texto de resultado de la intervención (fallback si cierre_supervisor está vacío) |
| `cierre_texto` | Columna auxiliar: cierre_supervisor si existe, sino resultado |
| `texto_limpio` | cierre_texto normalizado: minúsculas, sin tildes, sin puntuación |
| `categoria_final` | Categoría estandarizada resultante |
| `contacto` | Nivel de contacto derivado de categoria_final |
| `brinda_datos` | Si la persona brindó datos, derivado de categoria_final |

### Pipeline de categorización (`transformations.py`)

```
cierre_supervisor / resultado
        │
        ▼
limpiar_texto_cierre()
  → minúsculas, unidecode, sin puntuación, sin guiones
        │
        ▼
mapear_categoria_con_reglas()  — 3 niveles en orden de prioridad
  │
  ├─ Nivel 1: PATRONES_EXACTOS  (dict de texto exacto → categoría)
  │   Ej: "17 dipa derivacion a cis" → "traslado efectivo a cis"
  │
  ├─ Nivel 2: PATRONES_PERSONALIZADOS  (substring match)
  │   Ej: cualquier texto que contenga "derivacion a red" → "traslado/acompanamiento a otros efectores"
  │   Ej: cualquier texto que contenga "acepta cis" → "acepta cis pero no hay vacante"
  │
  └─ Nivel 3: Fuzzy match con rapidfuzz (WRatio ≥ 80)
      Compara contra las 15 categorías canónicas
      Si score < 80 → "sin_match"
        │
        ▼
obtener_niveles(categoria_final)
  → contacto:     "Contacta" | "No se contacta" | "Derivaciones/seguimientos"
  → brinda_datos: "Brinda datos" | "No brinda datos" | ""
```

### Categorías canónicas

**Contacta — Brinda datos:**
- `traslado efectivo a cis`
- `acepta cis pero no hay vacante`
- `se activa protocolo de salud mental`
- `derivacion a same`
- `traslado/acompanamiento a otros efectores`
- `mendicidad (menores de edad)`

**Contacta — No brinda datos:**
- `se realiza entrevista`
- `rechaza entrevista y se retira del lugar`
- `imposibilidad de abordaje por consumo`
- `rechaza entrevista y se queda en el lugar`
- `derivacion a espacio publico`
- `no se encuentra en situacion de calle`

**No se contacta:**
- `no se contacta y se observan pertenencias`
- `no se contacta y no se observan pertenencias`
- `sin cubrir`
- `desestimado (cartas 911 u otras areas)`

### Agregar nuevos patrones

Para cubrir textos que caen en `sin_match`, editar `core/transformations.py`:

- **`PATRONES_EXACTOS`**: para textos que deben mapearse de forma exacta (el texto completo debe coincidir exactamente con la clave, ya normalizado).
- **`PATRONES_PERSONALIZADOS`**: para textos que contienen una frase clave (match por substring). Es la opción más común para nuevos patrones.

Ejemplo para agregar un nuevo patrón substring:
```python
PATRONES_PERSONALIZADOS = {
    ...
    "mi frase clave": "categoria canonica",   # ← agregar acá
}
```

Después de agregar patrones, los registros históricos en Neon con `sin_match` que ahora matcheen **no se actualizan automáticamente** — son registros ya guardados. Los registros nuevos del próximo ETL sí usarán los patrones nuevos.

---

## Tipo Evolución

Clasifica cada intervención nueva comparando contra el historial en Neon:

| Valor | Condición |
|---|---|
| `Nuevos` | DNI nunca visto antes en Neon |
| `Recurrentes` | DNI ya existía y su última intervención fue en la **misma comuna** |
| `Migratorios` | DNI ya existía pero su última intervención fue en **otra comuna** |
| `Nuevo repetido` | Mismo DNI aparece varias veces en la misma semana; solo el primero cuenta como "Nuevos" |
| `No clasificable` | DNI inválido (NO BRINDO, extranjero, etc.) |

La consulta usa `DISTINCT ON ("DNI_categorizado") ORDER BY "Fecha Inicio" DESC` para obtener el último estado de cada DNI en O(N) en lugar de bajar todo el histórico.

---

## Apariciones

Cuenta cuántas veces intervino cada DNI válido desde el inicio del histórico (Neon + lote nuevo). Se consulta en chunks de 500 DNIs para no generar queries enormes.

Para DNIs inválidos (NO BRINDO, extranjeros, etc.) el valor es `0`.

---

## Base de datos (Neon)

- **Tabla principal**: `historico_limpio`
- **~399k registros** (a marzo 2026)
- Columnas clave: `Id Suceso`, `Fecha Inicio`, `DNI_categorizado`, `comuna_calculada`, `categoria_final`, `contacto`, `brinda_datos`, `Tipo_Evolucion`, `apariciones`

El append usa `COPY FROM STDIN` chunkeado (chunks de 50k filas) para máxima velocidad sin saturar el free tier de Neon.
