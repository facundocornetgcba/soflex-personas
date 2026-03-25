# Reporte Semanal - Que medimos y como lo medimos

## De donde vienen los datos?

Todos los reportes toman los datos del archivo 2025_historico_limpio.parquet, que es el backup en Google Drive de la base de datos central (Neon). Contiene el historial completo de intervenciones del anio, ya procesado y limpio.

Los reportes **no modifican** la base de datos. Solo leen y calculan.

---

## Los dos reportes

El sistema genera dos reportes HTML distintos:

| Reporte | Archivo generado | Script |
|---|---|---|
| Dashboard de indicadores clave | reporte_autom_bap.html | dashboard_generator.py |
| Intervenciones por origen y contacto | reporte_semanal_origen.html | reporte_semanal_origen.py |

Ambos se abren directamente en el navegador. Se generan corriendo el script correspondiente, no se actualizan solos.

---

## Filtros base que aplican a ambos reportes

Antes de calcular cualquier numero, se excluyen ciertos registros:

1. **Fecha invalida:** Registros donde Fecha Inicio no es una fecha valida o esta vacia. Son los primeros en caer antes de cualquier otro filtro.
2. **Id Suceso Asociado != 0:** Solo se conservan registros donde Id Suceso Asociado = 0, es decir, intervenciones directas sin suceso previo vinculado. **Este filtro aplica a ambos reportes** (no solo al reporte de origen).
3. **Origenes invalidos:** Registros con origen CORTAN y EQUIVOCADO (errores de carga).

El reporte de origen aplica un filtro adicional:

4. **Categorias invalidas:** Registros donde categoria_final = `sin_match` o `error de soflex`. Son registros que el ETL no pudo clasificar correctamente.

**Nota sobre agencias internas:** El dashboard_generator define una lista de agencias a excluir (DIPA I COMBATE, MAPA DE RIESGO - SEGUIMIENTO, DIPA II ZABALA, AREA OPERATIVA, SALUD MENTAL) pero ese filtro actualmente **no se aplica en el codigo**. Si el parquet ya viene sin esos registros es porque el ETL los elimina antes; si no, estan incluidos en el conteo.

---

## Reporte 1 - Dashboard de Indicadores Clave

### Que muestra?

Un panel interactivo con dos tablas y seis graficos. Siempre muestra las **ultimas 8 semanas** de datos. El usuario puede cambiar la vista por **comuna** usando un selector desplegable.

### Las tablas de indicadores

Cada tabla tiene 6 filas. La columna Linea Base tiene valores de referencia historicos para comparar el desempeno semanal actual.

#### 1. Intervenciones totales
Cantidad de registros del Excel procesados para esa comuna y semana. Incluye todo tipo de carta (automatica y manual).

#### 2. Derivaciones CIS
Cantidad de intervenciones cuyo resultado fue traslado efectivo al CIS (Centro de Integracion Social). Mide cuantas personas fueron trasladadas efectivamente a un centro de atencion.

#### 3. Llamados 108
Cantidad de intervenciones con Tipo Carta = AUTOMATICA. Las cartas automaticas son generadas a partir de llamados al 108 (linea de atencion a personas en situacion de calle). Mide el volumen de demanda que ingresa por esa via.

#### 4. % Se contacta
Sobre las cartas automaticas de esa semana, que porcentaje tuvo contacto efectivo con la persona. Se muestra como XX% (N) donde XX es el porcentaje y N el numero absoluto.

Como se clasifica:
- **Se contacta:** el equipo pudo interactuar con la persona, independientemente del resultado
- **No se contacta:** la persona no fue encontrada o no quiso interactuar
- **Sin cubrir:** la carta quedo sin atender. Las comunas 2 y 14 tienen logica especial y no se clasifican automaticamente como sin cubrir.

#### 5. % No se contacta
Proporcion de intervenciones automaticas donde no fue posible el contacto.

#### 6. % Sin cubrir
Proporcion de intervenciones automaticas que quedaron sin atender en la semana.

---

### Los graficos de evolucion de DNI

Dos graficos de barras apiladas, uno para **Comuna 2** y otro para **Comuna 14**, que muestran semana a semana cuantas personas son:

- **Nuevas** (verde): aparecen por primera vez en toda la historia de la base
- **Recurrentes** (azul): ya estuvieron en esa misma comuna en semanas anteriores
- **Migratorias** (naranja): ya estuvieron, pero en una comuna diferente

Esto permite ver si la poblacion en calle de una zona se mantiene estable, crece con caras nuevas, o hay movimiento entre zonas.

---

### Los graficos de entrevista y resultado final

Cuatro graficos que desglosan que paso dentro de las intervenciones donde hubo contacto, separando por tipo de carta (automatica vs manual).

**Entrevista:**

| Categoria | Que significa |
|---|---|
| Brinda DNI | La persona accedio a la entrevista y proporciono su DNI |
| No brinda | La persona accedio pero no dio su DNI |
| No realiza entrevista | Hubo contacto pero no se llego a entrevistar |

**Resultado final:**

| Categoria | Que significa |
|---|---|
| Derivado | Fue derivada a algun recurso (CIS, SAME, NNNYA, Salud Mental, etc.) |
| Se retira | Rechazo la intervencion y se fue del lugar |
| Se queda | Rechazo pero se quedo en el lugar |
| Espacio publico | Derivacion a espacio publico |
| Otros | Situaciones que no entran en las categorias anteriores |

---

## Reporte 2 - Intervenciones por Origen y Nivel de Contacto

### Que muestra?

Una tabla interactiva con las intervenciones desglosadas por origen de la demanda y nivel de contacto, para las ultimas 8 semanas. Tiene selector de comuna y filas que se pueden expandir/colapsar.

### Como se agrupan los origenes

Los registros del campo Origen se agrupan en tres grandes categorias:

| Grupo | Origenes incluidos |
|---|---|
| Gobierno Ciudad (Coop.) | 911, Organismos Publicos, BAP/MDR, BOTI, Subte, Espacios Publicos, Judiciales, Cajeros, Monitoreo 108, Seguimiento, Adicciones, Ninias/Ninios, Punto Politico |
| Vecino / ONG | Vecino, ONG, Gestion Colaborativa |
| Personas Sin Techo | Sin Techo, Espontaneo (la persona se acerco por cuenta propia) |

### Estructura de la tabla

    Intervenciones totales
    +-- Automaticas
    |   +-- Se contacta
    |   +-- No se contacta
    |   +-- Sin cubrir
    |   +-- Desestimado
    |   +-- Gobierno Ciudad (Coop.)
    |   |   +-- Se contacta
    |   |   +-- No se contacta
    |   |   +-- ...
    |   +-- Vecino / ONG
    |   +-- Personas Sin Techo
    +-- Manuales

Cada celda muestra el conteo del periodo. Las filas hijo muestran tambien el porcentaje que representan respecto a su fila padre.

### Niveles de contacto en este reporte

| Nivel | Como se determina |
|---|---|
| Se contacta | El campo nivel_contacto indica contacto efectivo |
| No se contacta | El campo nivel_contacto indica sin contacto |
| Sin cubrir | La categoria_final es sin cubrir (tiene prioridad sobre el resto) |
| Desestimado | El campo nivel_contacto indica desestimado |
| Seguimiento | Todo lo que no entra en los anteriores |

---

## Columnas clave que genera el ETL

Para entender los reportes, es util saber que representa cada columna calculada automaticamente:

| Columna | Que representa |
|---|---|
| comuna_calculada | Numero de comuna segun ubicacion geografica (1-15, o 14.5 para Palermo Norte) |
| DNI_categorizado | DNI limpio: numero si es valido, texto descriptivo si no |
| categoria_final | Resultado de la intervencion mapeado a una categoria estandar |
| nivel_contacto | Estado de alto nivel: Se contacta, No se contacta, Desestimado, Otro |
| contacto | Si hubo o no contacto con la persona |
| brinda_datos | Si la persona proporciono datos durante la intervencion |
| Tipo_Evolucion | Clasificacion del DNI respecto al historial: Nuevos / Recurrentes / Migratorios |
| apariciones | Cuantas veces aparecio ese DNI en toda la historia de la base |

---

## Frecuencia y actualizacion

- **ETL (main.py):** Se corre manualmente cada vez que se sube un Excel nuevo a Drive, tipicamente una vez por semana.
- **Reportes:** Se generan bajo demanda corriendo el script correspondiente. Siempre leen los datos mas recientes disponibles.
- **Ventana de analisis:** Siempre las ultimas 8 semanas. A medida que pasan las semanas la ventana se desplaza automaticamente.
