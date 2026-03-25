# Flujo ETL — Cómo funciona el sistema de carga de datos

## ¿Qué hace este sistema?

Cada semana, el equipo opera en la calle y registra intervenciones en un archivo Excel. Este sistema toma ese Excel, lo procesa automáticamente y actualiza la base de datos central con los nuevos registros. Solo agrega lo que es nuevo — no reprocesa todo desde cero cada vez.

---

## El flujo paso a paso

### Punto de entrada: `main.py`

Este es el único archivo que hay que correr manualmente cada semana:

```
python main.py
```

---

### Paso 1 — Autenticación con Google Drive

El sistema se conecta a Google Drive usando una cuenta de servicio (credenciales en `credentials.json`). Desde Drive descarga el Excel con los datos nuevos de la semana.

- **Carpeta que lee:** `01_insumos`
- **Busca:** el Excel más reciente subido a esa carpeta
- Si no hay ningún Excel, el sistema avisa y se detiene sin hacer nada

---

### Paso 2 — Watermark: ¿qué es nuevo?

Antes de procesar nada, el sistema consulta la base de datos (Neon PostgreSQL) y pregunta: **"¿cuál es la fecha más reciente que ya tenemos cargada?"**

Esa fecha se llama **watermark**. Solo se procesan registros con fecha *posterior* al watermark. Esto garantiza que:

- No se duplica ningún registro
- El proceso es rápido (solo trabaja sobre datos nuevos)
- Si es la primera vez que se corre, procesa todo el Excel completo

---

### Paso 3 — Procesamiento (data_processor.py)

Una vez filtrados los registros nuevos, pasan por un pipeline de transformaciones en secuencia:

#### 3a. Backup crudo en Drive
Antes de tocar nada, guarda los datos tal como vienen del Excel en un archivo de respaldo (2025_historico_v2.parquet) en Drive. Es una copia de seguridad sin procesar.

#### 3b. Geo-enriquecimiento
Cada intervención tiene coordenadas (latitud/longitud). El sistema hace un cruce espacial para determinar en qué **comuna** ocurrió:

1. Primero verifica si el punto cae dentro del polígono **Palermo Norte** → asigna el valor 14.5
2. Si no, cruza contra el mapa oficial de las **15 comunas de CABA** → asigna el número de comuna (1 al 15)
3. Si no tiene coordenadas o no cae en ningún polígono, queda sin comuna asignada

Los archivos geográficos están en la carpeta `assets/comunas/`.

#### 3c. Limpieza y categorización
- **DNI:** Se limpia y clasifica cada valor. Los DNIs válidos quedan como número (ej: 12345678). Los inválidos se reemplazan por etiquetas como NO BRINDO/NO VISIBLE o CONTACTO EXTRANJERO.
- **Nombres:** Se normalizan a mayúsculas sin caracteres especiales.
- **Agencias excluidas:** Se eliminan filas de agencias internas que no deben aparecer en el análisis (DIPA I COMBATE, MAPA DE RIESGO, etc.).
- **Categoría de cierre:** El texto libre del campo resultado/cierre se mapea a una categoría estándar usando tres niveles de coincidencia:
  1. Coincidencia exacta con patrones conocidos
  2. Coincidencia por subcadena de texto
  3. Coincidencia aproximada (fuzzy) con umbral del 80%

Las categorías finales determinan los campos categoria_final, nivel_contacto, contacto y brinda_datos.

#### 3d. Clasificación de Tipo_Evolucion
Para cada DNI válido, el sistema determina cómo clasifica a esa persona en relación al historial:

| Tipo | Condición |
|---|---|
| Nuevos | DNI que nunca apareció en la base histórica |
| Recurrentes | DNI ya visto, esta vez en la misma comuna |
| Migratorios | DNI ya visto, esta vez en una comuna diferente |
| Nuevo repetido | El mismo DNI "nuevo" aparece más de una vez en la misma semana |
| No clasificable | DNI inválido (no brindó datos) |

Para esto el sistema consulta solo el último estado conocido por DNI desde Neon (sin bajar toda la base), y simula la evolución semana a semana dentro del lote nuevo.

#### 3e. Apariciones acumuladas
Para cada DNI válido, cuenta cuántas veces apareció en total en toda la historia (base Neon + lote nuevo). Alimenta la columna apariciones.

---

### Paso 4 — Carga a la base de datos (Neon PostgreSQL)

Los registros procesados se insertan en la tabla historico_limpio usando el método más rápido de PostgreSQL (COPY FROM STDIN).

- Si la tabla no existe, la crea automáticamente
- Si las columnas cambiaron de nombre entre versiones del Excel, las normaliza
- Opera en bloques de 50.000 filas para no sobrecargar la conexión

---

### Paso 5 — Backup limpio en Drive

Finalmente actualiza el archivo de backup con los datos ya procesados (2025_historico_limpio.parquet) en la carpeta 02_base_datos de Drive.

**Neon es la fuente de verdad. Drive es el backup.**

---

## Resumen visual del flujo

```
python main.py
    |
    |-- 1. Autenticacion Google Drive
    |-- 2. Descarga Excel mas reciente (carpeta 01_insumos)
    |-- 3. Watermark desde Neon -> filtra solo datos nuevos
    |
    +-- data_processor.py
            |-- 3a. Backup crudo -> Drive (historico_v2.parquet)
            |-- 3b. Geo: asigna comuna por coordenadas
            |-- 3c. Limpieza: DNI, nombres, categoria de cierre
            |-- 3d. Tipo_Evolucion: Nuevo / Recurrente / Migratorio
            |-- 3e. Apariciones acumuladas por DNI
            |-- 4.  Carga a Neon (append ultrarrapido)
            +-- 5.  Backup limpio -> Drive (historico_limpio.parquet)
```

---

## Archivos clave

| Archivo | Rol |
|---|---|
| main.py | Punto de entrada. Orquesta todo el proceso. |
| data_processor.py | Pipeline completo de transformaciones y carga. |
| core/db_connections.py | Conexion y operaciones sobre Neon PostgreSQL. |
| core/drive_manager.py | Autenticacion y operaciones sobre Google Drive. |
| core/transformations.py | Funciones de limpieza, categorizacion de DNI y texto. |
| credentials.json | Credenciales de la cuenta de servicio de Google. |
| assets/comunas/ | Archivos geograficos: mapa de comunas + poligono Palermo Norte. |

---

## ¿Qué pasa si algo falla?

| Situacion | Comportamiento |
|---|---|
| Neon no responde | Reintenta hasta 3 veces con espera progresiva |
| No hay Excel en Drive | Avisa y sale sin modificar nada |
| No hay datos nuevos | Avisa y sale sin modificar nada |
| Falla el backup de Drive | Avisa pero no cancela; los datos ya estan en Neon |
| Falla el estado historico de DNIs | Clasifica sin historial (DNIs existentes salen como Nuevos); avisa en consola |

---

## Modulos de reporte (uso manual, independiente del ETL)

Ademas del ETL, existen dos scripts de reporte que se corren por separado cuando se necesita generar un dashboard:

| Script | Que genera |
|---|---|
| dashboard_generator.py | Dashboard HTML interactivo con indicadores por comuna (ultimas 8 semanas) |
| reporte_semanal_origen.py | Reporte HTML de intervenciones por origen y nivel de contacto |

Estos leen el parquet de Drive y generan archivos .html listos para abrir en el navegador. No modifican la base de datos.
