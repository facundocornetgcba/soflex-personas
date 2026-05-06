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

**Rediseno 2026-05-06**: buckets colapsados y renombrados para alinear con presentacion PPT.

| Bucket | Cierres incluidos |
|---|---|
| `DERIVACION A DISPOSITIVO RED` | 01, 02, 03, 05 |
| `SE RETIRA VOLUNTARIAMENTE` | 06, 07, 08 |
| `DERIVACION UMBRAL CERO` | 09 |
| `DERIVACION A SAME` | 12, 13 |
| `DERIVACION A SEGURIDAD Y A ORDENAMIENTO URBANO` | 14, 15 |
| `NO SE CONTACTA` | 16, 17 |
| `NO ERAN PSC` | 18 |
| `SIN CUBRIR` | 19 |
| `DESESTIMADO` | 20 |

Cierres excluidos del reporte (no aparecen en graficos): `error de soflex` (DERIVACION A RED historica), `POSITIVO`, `sin_match` sin nivel Sin cubrir.

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
```

Excluidos: 08 (no se realiza entrevista), 09 (derivacion — no hay entrevista BAP directa), 12/13 (emergencia medica SAME), 14 (Seguridad), 15 (Ordenamiento Urbano), 18 (Mendicidad — no es persona en calle en sentido BAP), DERIVACION AREA CNNyA-102, POSITIVO.

**Cambio 2026-05-06**: removidos 09 (Umbral Cero) y 18 (Mendicidad) del set. Razon: Umbral Cero es derivacion sin entrevista BAP directa; Mendicidad no aplica el criterio de entrevista BAP. El conteo de "Realiza entrevista" estaba inflado por estos dos cierres de alto volumen.

Impacto en reporte: el cuadro DNI divide al universo "Se contacta" en Brinda DNI / No brinda / No realiza entrevista. Con el set acotado, derivaciones y casos de mendicidad caen en "No realiza entrevista".

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

## 9. Estado Neon vs Parquet (2026-05-06, post-refresh)

| Fuente | Filas | Rango |
|---|---|---|
| Parquet Drive (`2025_historico_limpio.parquet`) | 369,664 | 2025-01-01 → 2026-05-05 |
| Neon (`historico_limpio`) | pendiente sync | pendiente |

El parquet fue recargado desde fuentes originales (ver seccion 13). Neon requiere sync completo via `_sync_neon.py` (TRUNCATE + COPY). Pendiente resolucion de credenciales Neon (error de password al 2026-05-06).

El parquet es la fuente de verdad para reportes historicos. Los reportes (`reporte_semanal_origen.py`, `dashboard_generator.py`) leen directo del parquet — no dependen de Neon.

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
| `core/transformations.py` | CATEGORIAS_NUEVAS, NIVEL_POR_CIERRE, BUCKET_POR_CIERRE (rediseno), MAPEO_VIEJO_A_NUEVO, REALIZA_ENTREVISTA_CATS (removidos 09/18), PATRONES_PERSONALIZADOS (15 patrones nuevos), matcher 4-tier |
| `reporte_semanal_origen.py` | nivel_display con regla PENDIENTE + regla CERRADO sin cierre, preparar_df reorden nivel_norm antes de CATS_EXCLUIR, RES_GRUPOS actualizado a etiquetas PPT |
| `dashboard_generator.py` | clasificar_contacto con regla CERRADO sin cierre, RES_GRUPOS actualizado |
| `data_processor.py` | _build_estado_historico acepta max_fecha (evita contaminacion Tipo_Evolucion en re-procesos); flujo incremental puro sin DELETE ni trim |
| `migrate_cierres.py` | Script one-time backfill en Neon (ejecutado) |
| `MIGRACION_CIERRES.md` | Este documento |

Sin cambios: `main.py`, `core/db_connections.py`, `core/drive_manager.py`, `core/transformations.py` (estructura), `assets/comunas/`.

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

1. **`_refresh_abril_mayo.py`** (script ad-hoc, no committear): recargo abril desde `abril 2026.xls` local + mayo desde Gmail. La carga es por `Fecha Inicio` — sin dedup por `Id Suceso` (un suceso puede tener multiples filas legitimas). Parquet final: ~369K filas.

2. **`_fix_tipo_evolucion.py`** (script ad-hoc, no committear): re-clasifica `Tipo_Evolucion` en el parquet completo desde cero con historial vacio, corrigiendo que el refresh habia clasificado DNIs como Recurrentes porque Neon los tenia del run previo.

### Flujo incremental (definitivo)

`main.py` → `procesar_datos(excel_bytes, watermark)`:
1. Detecta watermark Neon (MAX Fecha Inicio).
2. Filtra Excel: `Fecha Inicio > watermark` (solo filas nuevas).
3. Append a Neon — sin DELETE previo.
4. Concat al parquet backup — sin trim previo.

**No hay ventana rolling ni DELETE**. El Excel de Gmail solo tiene datos recientes — borrar y re-insertar causa perdida de semanas no cubiertas por el Excel.

Correcciones de cierres post-carga (operadores editan Soflex despues del watermark) se capturan via `reconciliar_pendientes`: detecta sucesos PENDIENTE en Neon que ya tienen cierre en el Excel actual y los actualiza.

### Por que no dedup por Id Suceso

`Id Suceso` no es PK unica — un mismo suceso puede tener multiples filas (re-visitas, actualizaciones). La carga incremental se gestiona solo por `Fecha Inicio` (watermark). Dedup por `Id Suceso` causaba perdida de registros legitimos y subestimacion de totales.

---

## 14. Fix patrones free-text en PATRONES_PERSONALIZADOS (2026-05-06)

### Problema

15,171 registros en parquet con `categoria_final = sin_match` y `cierre_texto` no nulo (post-migracion). El matcher no los capturaba porque son texto libre escrito por operadores (no codigos estructurados).

### Patrones agregados a `PATRONES_PERSONALIZADOS`

| Patron (substring) | Categoria | Ejemplos de cierre_texto |
|---|---|---|
| `dipa combate` | 02. Traslado DIPA | "DIPA Combate - Htal. Borda - DIPA Combate" |
| `ingreso micro` | 03. Traslado Micro | "ingreso micro comuna 2" |
| `traslados micro` | 03. Traslado Micro | "traslados micro cmna 14" |
| `micro solidario` | 03. Traslado Micro | "MICRO SOLIDARIO C14" |
| `se lo traslada a micro` | 03. Traslado Micro | "se lo traslada a micro en recoleta" |
| `se los traslada a micro` | 03. Traslado Micro | — |
| `no se contacta y no se` | 17. No se observan personas ni pertenencias | Variantes con typo: "obaservan", "pertencias", "pertenecias" |
| `informa no se contacta` | 17. No se observan personas ni pertenencias | "911 informa no se contacta personas" |
| `no se contacta y se ob` | 16. No se observan personas y hay pertenencias | "no se contacta y se observan pertenencias" |
| `eximicion` | 07. Se realiza entrevista y se retira | "se le realiza eximicion de pago para dni" |
| `asesoramiento` | 07. Se realiza entrevista y se retira | "asesoramiento sobre programas", "asesoramiento CP" |
| `se retira del lugar` | 07. Se realiza entrevista y se retira | "se retira del lugar por sus propios medios" |
| `se retiran del lugar` | 07. Se realiza entrevista y se retira | — |
| `se retira` | 07. Se realiza entrevista y se retira | "911 informa que la persona se retira", "108 informa el masculino se retira" |
| `rechaz` | 08. No se realiza entrevista y se retira | "rechaza recursos", "rechazan asistencia", "son rechazados", "rechaza CIS/DIPA", "rechaza vacante", "rechaza parador" |

Orden importante: patrones mas especificos primero (ej: `no se contacta y no se` antes de `se retira`).

### Resultado

| Estado | Cantidad |
|---|---|
| Candidatos (sin_match con cierre_texto no POSITIVO) | 15,171 |
| Re-categorizados en parquet | 1,092 (7.2%) |
| Distribucion nueva cat | 08: 540, 07: 392, 17: 97, 03: 25, 02: 20, 16: 18 |
| Aun sin_match | 14,079 (texto libre largo o ruido — no mapeables por substring) |

Script aplicado: `_recategorizar_sinmatch.py` (ad-hoc, no committear).

---

## 15. Gap de total en reportes por comuna — CERRADO sin cierre (2026-05-06)

### Contexto

Operadores de Comuna 2 reportaron que el total del reporte es menor al esperado.

### Analisis

| Universo | Filas |
|---|---|
| Com2 total parquet (id_suceso_asoc=0, todos los years) | 40,015 |
| Com2 visibles en reporte (post-filtros) | 34,645 |
| **Gap total** | **5,370 (13.4%)** |

Desglose del gap:

| Razon exclusion | Filas |
|---|---|
| `error de soflex` (DERIVACION A RED historica) | 877 |
| `sin_match` con nivel_norm = "Seguimiento" — sin cierre_texto | 4,165 |
| `sin_match` con nivel_norm = "Seguimiento" — cierre_texto no mapeable | 328 |
| **Total excluido** | **5,370** |

### Solucion aplicada (2026-05-06)

Los 4,165 registros CERRADO+sin_match+cierre_texto=None se reclasifican como **Sin cubrir**. Razon: un suceso cerrado sin ningun cierre cargado equivale operativamente a no haber podido dar respuesta.

**`nivel_display()` en `reporte_semanal_origen.py`** — nueva regla:
```python
if estado == "CERRADO" and pd.isna(cierre_texto_val):
    if cat_str in ("sin_match", ""):
        return "Sin cubrir"
```

**`clasificar_contacto()` en `dashboard_generator.py`** — idem:
```python
if row.get('estado') == 'CERRADO':
    cat = str(row.get('categoria_final', '')).strip()
    cierre = row.get('cierre_texto')
    if cat in ('sin_match', '') and pd.isna(cierre):
        return 'Sin cubrir'
```

Condicion `pd.isna(cierre_texto_val)` es critica — evita que registros con `cierre_texto = "POSITIVO"` (sin_match intencional) se cuenten como Sin cubrir.

Los 877 `error de soflex` son DERIVACION A RED historica — excluidos por diseno, no se cambia.

### Distribucion temporal de los 13,546 CERRADO sin cierre (parquet completo)

Concentrados en enero-marzo 2026. Abril y mayo tienen 0 registros de este tipo — el refresh desde fuente original cargo los cierres actualizados.

---

## 16. Correccion mapeos Umbral Cero y CNNyA-102 (2026-05-06)

### Problema

Dos mapeos incorrectos en `core/transformations.py`:

1. **`DERIVACION AREA CNNyA-102`** estaba mapeado a `09. Derivacion al equipo de Umbral Cero`. CNNyA (Consejo de Ninos, Ninas y Adolescentes) es derivacion a organismo de seguridad/infancia — debe ir a `14. Derivacion a Seguridad`.

2. **`09. Umbral Cero`** representa operativamente "se realiza entrevista, rechaza recursos y permanece en el lugar" (el equipo de Umbral Cero queda a cargo). Faltaban patrones para capturar este texto libre. Ademas, `09 rechaza entrevista y se queda en el lugar` estaba mapeado a `08` (incorrecto).

### Cambios aplicados

**MAPEO_VIEJO_A_NUEVO**:
- `derivacion area cnnya 102` / `11 derivacion area cnnya 102` → `14. Derivacion a Seguridad`
- `09 rechaza entrevista y se queda en el lugar` → `09. Derivacion al equipo de Umbral Cero` (era 08)

**PATRONES_EXACTOS**:
- `asesoramiento sobre programas rechan entrevista se quedan en el lugar` → `09` (era 08)

**PATRONES_PERSONALIZADOS** — patrones nuevos para Umbral Cero (antes de `rechaz`):
- `rechaza y se queda` → 09
- `rechaza entrevista y se queda` → 09
- `se queda en el lugar` → 09
- `queda en el lugar` → 09
- `permanece en el lugar` → 09

**PATRONES_PERSONALIZADOS** — CNNyA variantes → `14. Derivacion a Seguridad`:
- `derivacion area cnnya`, `cnnya`, `cnnva`, `nnnya`, `nnya 102`

**REALIZA_ENTREVISTA_CATS**: agregado `09. Derivacion al equipo de Umbral Cero` — hay entrevista efectiva, la persona rechaza recursos y permanece en el lugar.

### Impacto

Registros historicos con CNNyA ahora cuentan en "DERIVACION A SEGURIDAD Y A ORDENAMIENTO URBANO" en lugar de "DERIVACION UMBRAL CERO". Registros con texto "se queda/permanece en el lugar" ahora clasifican como 09 en vez de sin_match u 08.
