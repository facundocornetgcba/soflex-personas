# Migración de cierres y reestructura de indicadores

Documenta todos los cambios aplicados al pipeline `autom-bap-personas` desde abril 2026 para alinear la nomenclatura de cierres, los buckets de reporte y las reglas de Sin cubrir con el esquema canónico nuevo.

---

## 1. Contexto

La fuente de datos cambió la nomenclatura de la columna `resultado` (cierres de suceso). El esquema viejo usaba prefijos numéricos con guion (`01-`, `02-`, ...) y unos 16 valores. El esquema nuevo usa punto y espacio (`01.`, `02.`, ...) con 17 valores canónicos. A partir del 01/05/2026 los cierres `DERIVACION A RED`, `DERIVACION AREA CNNyA-102` y `POSITIVO` fueron eliminados del sistema Soflex.

El cambio de shape rompió el matcher de `core/transformations.py` (exact/substring/fuzzy), generando muchos `categoria_final = sin_match`. El reporte semanal descartaba esos registros en silencio; el dashboard los contaba como Se contacta por default. Los números no cerraban.

---

## 2. Mapeo viejo -> nuevo (backfill)

Aplicado sobre `historico_limpio` en Neon y sobre el parquet de Drive (`2025_historico_limpio.parquet`) mediante `migrate_cierres.py`.

| Valor viejo | Valor nuevo |
|---|---|
| `01-Traslado efectivo a CIS` | `01. Traslado efectivo a CIS` |
| `02-Acepta CIS pero no hay vacante` | `06. Acepta CIS pero no hay vacante` |
| `03-Se activa Protocolo de Salud Mental` | `13. Derivacion a SAME por salud mental` |
| `05-Derivacion a SAME` | `12. Derivacion a SAME por deterioro fisico visible` |
| `07-Rechaza entrevista y se retira del lugar` | `07. Se realiza entrevista y se retira del lugar` |
| `10-Derivacion a Espacio Publico` | `15. Derivacion a Ordenamiento Urbano` |
| `11-No se contacta y se observan pertenencias` | `16. No se observan personas y hay pertenencias` |
| `12-No se contacta y no se observan pertenencias` | `17. No se observan personas ni pertenencias` |
| `13-Mendicidad (menores de edad)` | `18. Mendicidad` |
| `15-Sin cubrir` | `19. Sin cubrir` |
| `16-Desestimado (cartas 911 u otras areas)` | `20. Desestimado` |
| `04-Traslado/acompanamiento a otros efectores` | NULL (sin mapeo) |
| `06-Se realiza entrevista` | `08. No se realiza entrevista y se retira del lugar` |
| `08-Imposibilidad de abordaje por consumo` | NULL |
| `09-Rechaza entrevista y se queda en el lugar` | `08. No se realiza entrevista y se retira del lugar` |
| `14-No se encuentra en situacion de calle` | NULL |
| `NEGATIVO` | NULL |

Valores legacy adicionales encontrados en datos (agrega a `MAPEO_VIEJO_A_NUEVO` en `core/transformations.py`):

| Valor legacy | Mapeo |
|---|---|
| `04 traslado efectivo 690` | `01. Traslado efectivo a CIS` |
| `11 derivacion area cnnya 102` | `DERIVACION AREA CNNyA-102` |
| `derivacion a ep` | `15. Derivacion a Ordenamiento Urbano` |

Filas con NULL post-mapeo: `categoria_final = sin_match`, `nivel_contacto = Sin dato`. Caen en "Cierres no identificables" en el reporte.

---

## 3. Lista canonica nueva (17 valores vigentes desde 01/05/2026)

```
01. Traslado efectivo a CIS
02. Traslado efectivo a DIPA
03. Traslado efectivo a Micro
05. Traslado efectivo a lugar de origen
06. Acepta CIS pero no hay vacante
07. Se realiza entrevista y se retira del lugar
08. No se realiza entrevista y se retira del lugar
09. Derivacion al equipo de Umbral Cero de Primer Abordaje
12. Derivacion a SAME por deterioro fisico visible
13. Derivacion a SAME por salud mental
14. Derivacion a Seguridad
15. Derivacion a Ordenamiento Urbano
16. No se observan personas y hay pertenencias
17. No se observan personas ni pertenencias
18. Mendicidad
19. Sin cubrir
20. Desestimado
```

Nota: numeros 04, 10, 11 no existen en el nuevo esquema.

### Cierres eliminados a partir del 01/05/2026

| Cierre eliminado | Destino historico en parquet | Registros afectados |
|---|---|---|
| `DERIVACION A RED` | `error de soflex` (excluido del reporte) | ~33,000 total / 11,467 en 2026 |
| `POSITIVO` | `sin_match` (excluido del reporte) | ~16,091 total |
| `DERIVACION AREA CNNyA-102` | `09. Derivacion al equipo de Umbral Cero de Primer Abordaje` | ~1,617 total |

Recompute aplicado sobre parquet 2026-05-06. Los 50,708 registros afectados fueron re-clasificados usando el matcher actual (cierre_supervisor primero, resultado como fallback).

---

## 4. Buckets de reporte (etiquetas PPT)

Definidos en `BUCKET_POR_CIERRE` en `core/transformations.py`. Todos los archivos de reporte y dashboard leen este dict — unica fuente de verdad.

| Bucket | Cierres incluidos |
|---|---|
| `SE DERIVA` | 01, 02, 03, 05, 09, 14, DERIVACION AREA CNNyA-102 |
| `CASOS DE SALUD MENTAL` | 12, 13 |
| `SE RETIRA` | 07, 08 |
| `ESPACIO PUBLICO` | 15 |
| `MENDICIDAD` | 18 |
| `ACEPTA CIS SIN VACANTE` | 06 |
| `NO SE CONTACTA` | 16, 17 |
| `SIN CUBRIR` | 19 |
| `DESESTIMADO` | 20 |
| `POSITIVO` | POSITIVO (excluido del reporte semanal) |
| `DERIVACION A RED` | DERIVACION A RED (excluido del reporte semanal) |
| `Cierres no identificables` | NULL / sin_match |

---

## 5. Nivel de contacto por cierre

Definido en `NIVEL_POR_CIERRE` en `core/transformations.py`.

| Nivel | Cierres |
|---|---|
| Se contacta | 01, 02, 03, 05, 06, 07, 08, 09, 12, 13, 14, 15, 18, DERIVACION AREA CNNyA-102, POSITIVO |
| No se contacta | 16, 17, DERIVACION A RED |
| Sin cubrir | 19 |
| Desestimado | 20 |
| Sin dato | NULL / sin_match |

---

## 6. Matcher de categorias — 4 tiers

Definido en `mapear_categoria_con_reglas()` en `core/transformations.py`.

- **Tier 0** — `MAPEO_VIEJO_A_NUEVO`: lookup exacto sobre texto limpiado (sin acentos, sin guiones, minusculas). Resuelve legacy y nuevos al canonico.
- **Tier 1** — `PATRONES_EXACTOS`: patrones de strings DIPA compuestos.
- **Tier 2** — `PATRONES_PERSONALIZADOS`: patrones substring/regex mas flexibles.
- **Tier 3** — rapidfuzz WRatio >= 80: matching difuso como fallback final.

Si ningun tier matchea: `categoria_final = sin_match`.

---

## 7. REALIZA_ENTREVISTA_CATS — set acotado

Definido en `core/transformations.py`. Solo cierres donde se hace interaccion directa solicitando datos de la persona:

```
01. Traslado efectivo a CIS
02. Traslado efectivo a DIPA
03. Traslado efectivo a Micro
05. Traslado efectivo a lugar de origen
06. Acepta CIS pero no hay vacante
07. Se realiza entrevista y se retira del lugar
09. Derivacion al equipo de Umbral Cero de Primer Abordaje
18. Mendicidad
```

Excluidos: 12/13 (emergencia medica SAME — no hay intercambio de datos), 14 (Seguridad), 15 (Ordenamiento Urbano), DERIVACION AREA CNNyA-102, POSITIVO, 08 (no se realiza entrevista).

Impacto en reporte: el cuadro DNI divide al universo "Se contacta" en Brinda DNI / No brinda / No realiza entrevista. Con el set acotado, derivaciones y emergencias caen en "No realiza entrevista" en lugar de inflar "No brinda".

---

## 8. Regla Sin cubrir — PENDIENTE en comunas no priorizadas

### Comunas priorizadas (cobertura 7x24)
`{2.0, 13.0, 14.0, 1.5}`

En estas comunas solo se contabiliza Sin cubrir cuando hay un cierre explicito `19. Sin cubrir`.

En el resto de comunas (1, 3-12, 15), un suceso con `Estado = PENDIENTE` (sin cierre) se considera Sin cubrir porque indica que el equipo no puede dar respuesta en tiempo real.

### Implementacion
`nivel_display()` en `reporte_semanal_origen.py`:

```python
if estado == "PENDIENTE":
    if float(comuna_val) not in COMUNAS_PRIORIZADAS:
        return "Sin cubrir"
```

### Orden de operaciones en preparar_df
La regla necesita ver los PENDIENTES ANTES de que el filtro `CATS_EXCLUIR` los descarte. Los PENDIENTES tienen `categoria_final = sin_match` (no hay cierre). Si se filtra primero, la regla nunca ejecuta.

Orden correcto (aplicado en `preparar_df`):
1. Computar `nivel_norm` para todas las filas (incluye PENDIENTES con sin_match).
2. Aplicar `CATS_EXCLUIR` excepto para filas con `nivel_norm == "Sin cubrir"`.

```python
mask_excluir = df["categoria_final"].isin({"error de soflex", "POSITIVO", "DERIVACION A RED"}) | (
    (df["categoria_final"] == "sin_match") & (df["nivel_norm"] != "Sin cubrir")
)
```

### Por que Sin cubrir baja en semanas recientes — lagging indicator

Sin cubrir es un **indicador rezagado**. El equipo carga los cierres `19. Sin cubrir` con demora de 2 a 7 dias. El reporte puede mostrar valores muy bajos en la ultima semana porque:

1. Los sucesos aun estan `PENDIENTE` (sin cierre definitivo). La regla PENDIENTE + no priorizada los captura y suma.
2. Algunos CERRADO con cierre `19.` todavia no fueron ingresados.

Esto es comportamiento esperado, no un bug. El numero de la semana actual sube a medida que pasan los dias.

Serie historica de referencia (parquet 2026-05-06, 466,420 filas):

| Semana     | Sin cubrir |
|---|---|
| 2026-01-05 | 1,401 |
| 2026-02-09 | 2,068 |
| 2026-03-02 | 1,876 |
| 2026-03-09 | 1,443 |
| 2026-03-30 |   390 (semana corta por feriado) |
| 2026-04-13 | 1,509 |
| 2026-04-20 | 1,609 |
| 2026-04-27 |   669 (lagging — semana en transcurso al 05-06) |
| 2026-05-04 |   675 (lagging — semana en transcurso al 05-06) |

Semanas con valor bajo (< 500) = semana aun sin todos los cierres cargados. No es undercount estructural.

### Nota sobre sin_match con Cierre Supervisor interno de Soflex

Algunos registros tienen `Cierre Supervisor` = "Suceso [S108:XXXXXXX] asociado al suceso [S108:YYYYYYY]". Es un mensaje interno de Soflex, no un cierre real. El matcher los clasifica como `sin_match` correctamente — no hay forma de mapear ese texto a un cierre canonico. Estos registros no aparecen en el reporte (filtrados como sin_match sin nivel_norm == Sin cubrir).

---

## 9. Estado Neon vs Parquet (2026-05-06)

| Fuente | Filas | Rango |
|---|---|---|
| Parquet Drive (`2025_historico_limpio.parquet`) | 466,420 | 2025-01-01 → 2026-05-05 |
| Neon (`historico_limpio`) | 4,218 | 2026-05-01 → 2026-05-05 |

**Neon solo tiene datos de mayo 2026** porque el proyecto Neon fue recreado vacio y `main.py` cargo solo el ultimo Excel disponible. El parquet es la fuente de verdad para reportes historicos.

Para cargar el historico completo a Neon: ejecutar `_load_parquet_to_neon.py` (psycopg2 directo, preserva nombres de columna originales) antes de correr `main.py`. Despues `main.py` corre incremental sobre el watermark del parquet completo.

---

## 10. Recompute del parquet

El parquet `2025_historico_limpio.parquet` en Drive fue recomputado inline para actualizar las columnas derivadas (`cierre_texto`, `texto_limpio`, `categoria_final`, `nivel_contacto`, `contacto`, `brinda_datos`) con el matcher nuevo.

Columnas recalculadas:
```
cierre_texto    = Cierre Supervisor si no nulo, sino resultado
texto_limpio    = limpiar_texto_cierre(cierre_texto)
categoria_final = mapear_categoria_con_reglas(texto_limpio)
nivel_contacto  = obtener_nivel_contacto(categoria_final)
contacto, brinda_datos = obtener_niveles(categoria_final)
```

Total filas procesadas: 466,420 (post main.py 2026-05-06). sin_match: ~75,325.
- ~44,000 son PENDIENTE sin cierre (sin_match esperado, rescatados por regla Sin cubrir si corresponde).
- ~16,900 tienen `resultado=POSITIVO` sin cierre supervisor — sin_match intencional, excluidos del reporte.
- ~12,900 tienen `Cierre Supervisor` con texto interno de Soflex ("Suceso [S108:...]") — sin_match legitimo.

Nota: el campo `resultado` es la carga raw del Excel (no se modifica en el recompute del parquet). Solo se modifica en Neon via `migrate_cierres.py`.

---

## 11. Archivos modificados

| Archivo | Cambios |
|---|---|
| `core/transformations.py` | CATEGORIAS_NUEVAS, NIVEL_POR_CIERRE, BUCKET_POR_CIERRE, MAPEO_VIEJO_A_NUEVO, REALIZA_ENTREVISTA_CATS, matcher 4-tier |
| `reporte_semanal_origen.py` | nivel_display con regla PENDIENTE, preparar_df reorden nivel_norm antes de CATS_EXCLUIR, _clasificar_resultado delega a BUCKET_POR_CIERRE, RES_GRUPOS actualizado a etiquetas PPT |
| `dashboard_generator.py` | _clasificar_resultado delega a BUCKET_POR_CIERRE, RES_GRUPOS actualizado |
| `migrate_cierres.py` | Script one-time backfill en Neon (ejecutado) |

Sin cambios: `main.py`, `data_processor.py`, `core/db_connections.py`, `assets/comunas/`.

---

## 12. Como correr los reportes standalone

```bash
# Reporte semanal (genera reporte_semanal_origen.html)
python reporte_semanal_origen.py

# Dashboard (genera dashboard.html)
python dashboard_generator.py
```

Ambos leen directamente de `2025_historico_limpio.parquet` en Drive (no necesitan Neon).

Para re-ejecutar el pipeline completo (nuevo Excel de Soflex → append a Neon + parquet):
```bash
python main.py
```

Requiere Neon con espacio disponible. Si Neon esta lleno, el pipeline falla en el append pero los reportes siguen funcionando contra el parquet.

---

## 13. Bug: parquet stale por correcciones operativas + duplicados (detectado 2026-05-06)

### Problema

Cruce `abril 2026.xls` vs parquet por `Id Suceso` revelo dos bugs:

**A — Duplicados**: parquet tenia 466,420 filas / 366,140 ids unicos (~100K duplicados). `main.py` append-only re-procesaba Excels solapados sin dedupe.

**B — Cierres stale**: De 6,829 filas con `Resultado=15-Sin cubrir` en Excel abril:
- 3,656 OK en parquet (correcto)
- 3,162 con `resultado=None` en parquet
- 1,722 con `resultado=DERIVACION A RED` en parquet

Causa: operadores corrigen cierres en Soflex post-load (ej: DERIVACION A RED → 15-Sin cubrir). El watermark de `main.py` impedia re-leer esas filas.

Impacto en Sin cubrir:

| Semana | Antes (parquet stale) | Despues (fix) | Excel raw |
|---|---|---|---|
| 2026-03-30 | 114 | 1,195 | 1,137 |
| 2026-04-06 | 498 | 1,866 | 1,928 |
| 2026-04-13 | 1,509 | 1,726 | 1,779 |
| 2026-04-20 | 1,609 | 1,773 | 1,828 |
| 2026-04-27 | 494 | 982 | ~677+ |

### Correcciones aplicadas (2026-05-06)

1. **`_refresh_abril_mayo.py`** (script ad-hoc, no committear): recargo abril desde `abril 2026.xls` local + mayo desde Gmail. Dedup global por Id Suceso. Parquet final: 366,140 filas sin duplicados.

2. **`main.py` ventana refresh 30 dias**: cada run elimina de Neon los ultimos 30 dias y los re-procesa desde el Excel. Captura correcciones operativas con demora de hasta 30 dias.

### Ventana de refresh

`REFRESH_DAYS = 30` en `main.py`. Cada run de `main.py`:
1. Detecta watermark Neon.
2. Calcula `refresh_from = watermark - 30 dias`.
3. DELETE FROM historico_limpio WHERE "Fecha Inicio" >= refresh_from.
4. Re-procesa registros desde `refresh_from` (incluye ventana + nuevos).

Overhead por run: ~30 dias de sucesos extra en procesamiento y re-insert.
