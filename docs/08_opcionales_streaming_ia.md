# Componentes opcionales

## A. Procesamiento en tiempo casi real (movimientos de pólizas y siniestros)
PoC en `notebooks/10_streaming_autoloader_demo.py`. Diseño corporativo:

| Tema | Diseño |
|---|---|
| Fuente | Core de pólizas y siniestros publica eventos en **Azure Event Hubs** (protocolo Kafka); un tópico por dominio |
| Contrato del evento | `evento_id` (UUID, único), `entidad`, `id_entidad`, `tipo_evento`, `fecha_evento` (timestamp de negocio), `fecha_emision`, `secuencia` (por entidad), `version_esquema`, `payload` (JSON con esquema registrado en un Schema Registry). Compatibilidad *backward* obligatoria |
| Ingesta | Structured Streaming (o Lakeflow Declarative Pipelines) → `bronze_movimientos_stream`, append-only, con offset, partición y timestamp de Kafka |
| Checkpointing y garantía | Checkpoint en ADLS por stream: offsets + estado. Fuente reproducible + sink Delta transaccional = **exactamente una vez** de extremo a extremo |
| Duplicados | `dropDuplicatesWithinWatermark("evento_id")` en Bronze → Silver + `MERGE` por `evento_id` (idempotente ante reintentos del productor) |
| Tardíos y fuera de orden | Watermark de **48 h** sobre `fecha_evento`. El estado vigente de la entidad se calcula ordenando por (`fecha_evento`, `secuencia`), no por llegada. El MERGE solo actualiza si (`fecha_evento`, `secuencia`) entrante > actual |
| Eventos más allá del watermark | No se descartan: van a Bronze y a la tabla `movimientos_tardios`, que el batch mensual reconcilia (backfill del corte) |
| Dead-letter queue | Eventos sin clave, con payload inválido o con versión de esquema desconocida → `stream_movimientos_dlq` con motivo, offset y `batch_id`; alerta si superan el 1% por micro-batch |
| Integración Silver/Gold | `foreachBatch` → MERGE en `silver_movimientos` → Change Data Feed alimenta indicadores operativos casi en tiempo real (p. ej. siniestros abiertos del día). Los productos mensuales certificados siguen saliendo del batch |
| Observabilidad | `StreamingQueryListener` → tabla de métricas (filas/s, latencia, retraso de offsets, tamaño del estado); alertas por *consumer lag* y por stream detenido |
| Recuperación | Reinicio desde el checkpoint; reproceso total borrando el checkpoint y releyendo Bronze (retención de Event Hubs ≥ 7 días + Bronze como fuente de verdad) |
| Costo y operación | Micro-batches `availableNow` cada 5–15 min en serverless (costo ∝ uso) frente a un stream continuo 24/7. Elegir según el SLA real del caso de uso; para reportes mensuales no se justifica el continuo |
| Limitación Free Edition | Solo `trigger(availableNow=True)`, sin Event Hubs: la PoC simula el buzón de eventos con archivos y Auto Loader |

## B. Habilitación para IA generativa (experiencia conversacional gobernada)

| Necesidad | Preparación de datos y controles |
|---|---|
| Datasets y vistas autorizadas | Solo `vw_gold_poliza_360_certificada`, `vw_gold_siniestralidad_certificada`, `gold_discrepancias` y un resumen de `gold_calidad_pipeline`. **Nunca** Bronze, cuarentena ni tablas con PII |
| Semántica y metadatos | Comentarios de tabla y columna en UC (ya incluidos), glosario (prima devengada, costo incurrido, razón de siniestralidad), métricas definidas una sola vez (UC *metric views*) y consultas de ejemplo verificadas (*trusted assets*) en un **Genie space** |
| Control de acceso por usuario | El agente consulta **con la identidad del usuario** (*on-behalf-of*): aplican los GRANT, *row filters* (p. ej. por sucursal del usuario) y máscaras de UC; nada de credenciales compartidas |
| Protección de información sensible | Gold sin PII; máscaras de columna; *guardrails* de entrada y salida (detección de PII, temas prohibidos) en AI Gateway |
| Frescura y trazabilidad | Cada respuesta incluye `fecha_corte` y `run_id_publicacion` (columnas de las vistas certificadas) y cita las tablas y consultas usadas |
| Evitar datos no certificados | El agente solo tiene acceso a las vistas `*_certificada`, que exponen únicamente cortes en estado **PUBLICADO**; las tablas en proceso no son visibles. Si la última ejecución falló, responde con el último corte válido y lo advierte |
| Auditoría de consultas y costos | AI Gateway con *inference tables* (prompt, respuesta, usuario, latencia, tokens), `system.access.audit` para el SQL ejecutado, límites de tasa y presupuestos por grupo; evaluación periódica de calidad de respuestas con un set de preguntas de referencia |
