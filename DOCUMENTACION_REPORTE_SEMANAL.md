# Documentación — Reporte Semanal de Intervenciones por Origen

## ¿Qué es este reporte?

Es un reporte que muestra **cuántas intervenciones del BAP (Buscando y Acompañando Personas) se hicieron cada semana**, con dos secciones:

1. **Una tabla** que desagrega las intervenciones por quién las originó y qué resultado tuvieron.
2. **Cuatro gráficos de barras** que analizan en detalle qué pasó con las personas con las que el equipo logró contacto.

El reporte se genera corriendo `reporte_semanal_origen.py` y produce un archivo `reporte_semanal_origen.html` que se abre en cualquier navegador.

---

## Cómo se lee la tabla

### Las columnas son semanas

Cada columna representa una semana. El formato es `DD/MM` y corresponde al **lunes** de esa semana. Las semanas se muestran de más antigua (izquierda) a más reciente (derecha). Por defecto se muestran las últimas 8 semanas.

Ejemplo:
```
Dimensión              | 03/02 | 10/02 | 17/02 | 24/02 | ...
```

### Las filas son jerárquicas (tienen niveles)

La tabla tiene una estructura en árbol. Cada fila indentada es un **subconjunto** de la fila de arriba.

```
Intervenciones totales              ← todo junto
  ▼ Automáticas                     ← solo las automáticas
      · Se contacta   (total auto)  ← NUEVO: totales de nivel para todas las automáticas
      · No se contacta
      · Sin cubrir
      · Desestimado
      · Seguimiento
      ▼ Gobierno Ciudad             ← automáticas originadas por el gobierno
          · Se contacta             ← de esas, cuántas lograron contacto
          · No se contacta
          · Sin cubrir
          · Desestimado
          · Seguimiento
      ▼ Vecino / ONG
          · Se contacta
          ...
      ▼ Personas Sin Techo
          · Se contacta
          ...
  ▼ Manuales                        ← solo las manuales
```

> **Importante:** los números de los sub-niveles suman la fila padre. Por ejemplo, "Se contacta" + "No se contacta" + "Sin cubrir" + "Desestimado" + "Seguimiento" dentro de "Gobierno Ciudad" deben dar el total de "Gobierno Ciudad".

### Los porcentajes

Cada celda muestra el número y, entre paréntesis, **el porcentaje que representa sobre su fila padre**. Ejemplos:
- La fila "Gobierno Ciudad" muestra `320 (68%)` → significa que el 68% de las Automáticas esa semana vinieron de canales del Gobierno de la Ciudad.
- La fila "Se contacta" bajo "Gobierno Ciudad" muestra `180 (56%)` → el 56% de las intervenciones de Gobierno Ciudad lograron contacto.

Esto permite comparar el peso relativo de cada categoría de un vistazo, sin necesidad de hacer cuentas manualmente.

### Las filas se pueden expandir y colapsar

Las filas que tienen sub-filas muestran una **flecha ▼** al lado del nombre. Al hacer clic en esa flecha:
- La flecha rota a **▶** y los sub-niveles desaparecen (se colapsan).
- Hacer clic de nuevo vuelve a mostrarlos.

Esto es útil para simplificar la vista cuando solo interesa ver los totales sin el detalle.

---

## El filtro de comunas

En la esquina superior derecha del reporte hay un menú desplegable que dice **"Filtrar por comuna"**.

- **Todas las comunas** (opción por defecto): muestra los datos de toda la Ciudad de Buenos Aires.
- **Comuna 1, Comuna 2, ..., Comuna 15**: filtra y muestra solo las intervenciones que ocurrieron en esa comuna.
- **Palermo Norte (14.5)**: zona especial que el sistema georreferencia por separado del resto de la Comuna 14.

Al cambiar la selección, **tanto la tabla como los cuatro gráficos** se actualizan instantáneamente sin recargar la página. Todo está pre-calculado por comuna dentro del archivo HTML.

---

## Qué registros se incluyen y cuáles no

### Se excluyen los registros con "Id Suceso Asociado" distinto de cero

Cada intervención puede estar "asociada" a otro suceso previo (por ejemplo, un seguimiento de algo que ya estaba abierto). Cuando `Id Suceso Asociado` tiene un número distinto de cero, significa que **esa intervención es dependiente de otra**. El reporte excluye esos casos para contar solo intervenciones independientes/nuevas.

En la práctica, del total de registros históricos, la gran mayoría (≈97%) tienen `Id Suceso Asociado = 0` y sí se incluyen.

### Se excluyen los orígenes "EQUIVOCADO" y "CORTAN"

Son registros ingresados por error o que corresponden a llamadas equivocadas. No representan intervenciones reales y se descartan.

---

## Qué significa cada fila

### Intervenciones totales
El conteo de **todas las intervenciones** del período, después de aplicar los filtros de arriba. Es el número más grande de la tabla y representa el volumen total de trabajo de cada semana.

---

### Automáticas
Intervenciones donde **el sistema generó la orden de salida de forma automática** a partir de una denuncia o alerta recibida (por teléfono, chatbot, organismo, etc.). Son el grueso de las intervenciones.

Las automáticas se dividen en tres grupos según quién reportó la situación:

#### ◦ Gobierno Ciudad (Coop.)
Intervenciones que llegaron a través de canales del Gobierno de la Ciudad de Buenos Aires. Incluye:
- **911** — llamados a emergencias
- **Organismos Públicos** — derivaciones de otros organismos del Estado
- **Desde BAP/MDR** — generadas internamente desde el propio BAP o el Mapa de Riesgo
- **BOTI** — el chatbot oficial de la Ciudad (Buenos Aires en Tu Idioma)
- **Subte** — reportes desde el sistema de subterráneos
- **Espacios Públicos** — monitoreo de espacios públicos por la Ciudad
- **Judiciales** — derivaciones del Poder Judicial
- **Cajeros** — reportes desde zonas de cajeros automáticos
- **Monitoreo 108** — monitoreo desde la línea 108
- **Adicciones** — derivaciones por problemáticas de adicciones
- **Seguimiento** — casos en seguimiento por la Ciudad
- **Niña/Niño** — casos que involucran a menores de edad
- **Punto Político / Ingreso por fuera del 108** — reportes de otros canales de gobierno

#### ◦ Vecino / ONG
Intervenciones que llegaron porque **un vecino o una organización de la sociedad civil** reportó la situación. Incluye:
- **Vecino** — llamado o reporte de un vecino de la Ciudad
- **ONG** — reporte de una organización no gubernamental
- **Gestión Colaborativa** — iniciativas conjuntas entre ciudadanos y el gobierno

#### ◦ Personas Sin Techo
Intervenciones donde **la propia persona en situación de calle** tomó contacto con el servicio. Incluye:
- **Sin Techo** — la persona se identificó como persona sin techo
- **Espontáneo** — la persona se acercó espontáneamente al equipo o al servicio

---

### Manuales
Intervenciones donde **el operador creó el registro de forma manual**, sin que lo disparara automáticamente el sistema. Suelen ser situaciones detectadas directamente por los equipos en el territorio o registros de seguimiento administrativo.

> Las Manuales no se desagregan por origen ni por nivel de contacto porque ese dato no es consistente en este tipo de registros.

---

## Qué significa cada nivel de contacto

Dentro de cada grupo de origen (solo para las Automáticas), se muestran cinco niveles que indican **qué resultado tuvo la intervención**:

| Nivel | Qué significa |
|---|---|
| **Se contacta** | El equipo llegó al lugar y logró hablar con la persona. Incluye casos donde la persona aceptó o rechazó la ayuda, pero hubo contacto real. |
| **No se contacta** | El equipo llegó al lugar pero **no encontró** a la persona o no pudo establecer contacto (por ejemplo, la persona no estaba o no respondió). |
| **Sin cubrir** | La intervención **no pudo llevarse a cabo**: el equipo no llegó al lugar o el caso quedó sin atender por falta de recursos, distancia u otro motivo operativo. |
| **Desestimado** | La situación reportada se evaluó y se determinó que **no corresponde** una intervención del BAP (por ejemplo, no era una persona en situación de calle, era un llamado duplicado, o la situación fue resuelta por otra área como el 911). |
| **Seguimiento** | Todo lo que no entra en las categorías anteriores. Puede incluir registros incompletos, casos aún en proceso, o situaciones no clasificadas correctamente en el sistema. |

---

---

## Los gráficos de análisis de contacto

Debajo de la tabla hay **cuatro gráficos de barras apiladas**. Todos muestran las últimas 8 semanas y todos responden al filtro de comuna.

La base de todos los gráficos es la misma: **únicamente las intervenciones donde el equipo logró contacto con la persona** (es decir, las que en la tabla figuran como "Se contacta"). Se excluyen "No se contacta", "Sin cubrir", "Desestimado" y "Seguimiento", porque en esos casos no hubo interacción real con la persona.

Cada gráfico muestra el total apilado arriba de cada barra para facilitar la comparación semanal.

---

### Gráfico 1 — Entrevista: Automáticas (Se Contacta)
### Gráfico 2 — Entrevista: Manuales (Se Contacta)

Estos dos gráficos responden a la pregunta: **¿se realizó una entrevista con la persona?** Y si se realizó, **¿brindó su DNI?**

Cada barra semanal está dividida en tres segmentos:

| Segmento | Color | Qué significa |
|---|---|---|
| **Brinda DNI** | 🟢 Verde | El equipo realizó la entrevista **y** la persona proporcionó un número de DNI válido (entre 6 y 10 dígitos). Implica que hay registro identificable de la persona. |
| **No brinda** | 🔵 Azul | El equipo realizó la entrevista pero la persona **no proporcionó un DNI válido**: dijo no tenerlo, no recordarlo, se negó a darlo, o es extranjera. La entrevista ocurrió igual. |
| **No realiza entrevista** | 🟠 Naranja | El equipo llegó y tuvo contacto visual o verbal con la persona, pero **no se pudo concretar una entrevista**: la persona rechazó la intervención y se retiró o se quedó, hubo imposibilidad de abordaje por consumo problemático, fue derivada a espacio público, o no se encontraba en situación de calle. |

> **Cómo leerlo:** si en una semana la barra de "Automáticas" tiene 200 contactos y el segmento verde es la mitad, significa que 100 personas brindaron su DNI ese semana. Un crecimiento del segmento naranja puede indicar más situaciones complejas de abordar.

La diferencia entre el gráfico de Automáticas y el de Manuales permite ver si el comportamiento varía según cómo se originó la intervención.

---

### Gráfico 3 — Resultado Final: Automáticas (Se Contacta)
### Gráfico 4 — Resultado Final: Manuales (Se Contacta)

Estos dos gráficos responden a la pregunta: **¿qué pasó concretamente con la persona luego del contacto?** Se enfoca en el resultado operativo de la intervención.

Cada barra semanal está dividida en cinco segmentos:

| Segmento | Color | Qué significa |
|---|---|---|
| **Derivado** | 🟣 Violeta | La persona fue **derivada o trasladada a algún servicio o efector**. Incluye: traslado efectivo a un Centro de Inclusión Social (CIS), derivación al SAME, traslado a otros efectores de salud o sociales, activación de protocolo de salud mental, derivación a centro de niñas/niños/adolescentes (CNNyA/102), o casos de mendicidad infantil. Es el resultado más "activo" de la intervención. |
| **Se retira** | 🔴 Rojo | La persona **rechazó la entrevista y se retiró del lugar** voluntariamente. Hubo contacto, pero la persona decidió no participar y se fue. |
| **Se queda** | 🟡 Amarillo | La persona **rechazó la entrevista pero permaneció en el lugar**, o se realizó la entrevista sin derivación y la persona continuó en la calle. El equipo documentó la situación pero no hubo una acción concreta de traslado ni la persona se fue. |
| **Espacio público** | 🔵 Celeste | La persona fue **derivada a un espacio público de la Ciudad** (como un Parador u otro dispositivo de baja exigencia o de día). Es una derivación, pero a un recurso más liviano que un CIS. |
| **Otros** | ⬜ Gris | Casos que no entran en ninguna de las categorías anteriores: situaciones con información incompleta, registros sin categoría de cierre asignada, imposibilidad de abordaje por consumo donde no hubo derivación, o personas que resultaron no estar en situación de calle. |

> **Cómo leerlo:** un aumento del segmento violeta ("Derivado") es una señal positiva: más personas llegaron a algún servicio. Un aumento del segmento rojo ("Se retira") o gris ("Otros") puede indicar mayor resistencia al abordaje o registros incompletos que merecen revisión.

> **Nota:** "Derivado" y "Se queda" no son excluyentes de "Brinda DNI" o "No brinda" — son dos dimensiones distintas. Una persona puede haber dado su DNI y aun así haberse retirado, o no haber dado su DNI y haber sido derivada.

---

## Cómo se genera el reporte

1. El script `reporte_semanal_origen.py` se conecta a Google Drive y descarga el archivo `2025_historico_limpio.parquet` (la base de datos histórica del BAP).
2. Aplica los filtros (excluye `Id Suceso Asociado ≠ 0`, excluye EQUIVOCADO/CORTAN).
3. Agrupa por semana, tipo de carta, origen y nivel de contacto para construir la tabla.
4. Calcula los datos de los cuatro gráficos (entrevista y resultado final) para las cartas contactadas.
5. Repite todos los cálculos **para cada comuna por separado** y para el total ciudad.
6. Genera el archivo HTML con todos los datos embebidos. El filtro de comunas actualiza tabla y gráficos sin necesidad de internet ni servidor.

Para generar el reporte:
```bash
python reporte_semanal_origen.py
```

El archivo `reporte_semanal_origen.html` se crea en la misma carpeta y puede abrirse con cualquier navegador (Chrome, Edge, Firefox). Los gráficos requieren conexión a internet la primera vez que se abre el archivo (cargan Chart.js desde CDN); una vez cargados, funcionan sin conexión.

---

## Preguntas frecuentes

**¿Por qué los números de "Se contacta" + "No se contacta" + "Sin cubrir" + "Desestimado" + "Seguimiento" no suman exactamente el total del grupo?**
Puede haber una pequeña diferencia si hay registros sin categoría de cierre asignada, que van a "Seguimiento".

**¿Por qué algunas celdas muestran "–" en lugar de un número?**
El "–" significa cero: no hubo ninguna intervención de ese tipo en esa semana. Se muestra así para que la tabla sea más fácil de leer visualmente.

**¿Las semanas empiezan el lunes o el domingo?**
La semana empieza el **lunes**. La fecha que aparece en la cabecera de cada columna es el lunes de esa semana.

**¿Cada cuánto se actualiza el reporte?**
El reporte se actualiza cada vez que alguien corre el script manualmente. La fecha y hora de la última actualización aparece en el encabezado del reporte.

**¿Qué pasa si una intervención no tiene comuna asignada?**
Esa intervención aparece en "Todas las comunas" pero no en ninguna comuna individual. El sistema de georreferenciación puede no asignar comuna cuando las coordenadas están fuera del perímetro de la Ciudad o son inválidas.

**¿Cuántas semanas muestra el reporte?**
Tanto la tabla como los gráficos muestran las últimas **8 semanas**.

**¿Los gráficos incluyen las manuales en el análisis de entrevista y resultado?**
Sí. Hay un gráfico de entrevista y uno de resultado para Automáticas, y los mismos dos para Manuales. Sin embargo, el volumen de las Manuales suele ser mucho menor, por lo que sus barras son más bajas.

**¿Qué significa "Brinda DNI" exactamente?**
Que el campo DNI de la persona tiene un número válido: entre 6 y 10 dígitos numéricos. No incluye registros donde el campo dice "No brindó", "No visible", "Extranjero", letras, o está vacío. "No brinda" agrupa todos esos casos: la entrevista sí ocurrió, pero no quedó un DNI identificable.

**¿"Derivado" en los gráficos es lo mismo que "Derivaciones CIS" en otras partes del sistema?**
No exactamente. "Derivado" en estos gráficos es más amplio: incluye traslados a CIS, derivaciones a SAME, otros efectores de salud, protocolo de salud mental, y derivaciones a centros de niños (CNNyA). "Derivaciones CIS" en otros reportes se refiere únicamente al traslado efectivo a un Centro de Inclusión Social.
