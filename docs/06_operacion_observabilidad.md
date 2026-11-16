# Orquestación, operación y observabilidad

## 1. Workflow
Definición: `resources/jobs/pipeline_polizas_siniestros.job.yml`.

`bronze → silver → gold → validar_publicar → gobierno_y_vistas`

| Aspecto | Configuración |
|---|---|
| Programación | Quartz `0 0 6 5 * ?`, zona `America/Bogota` (día 5 de cada mes, 06:00 a. m. COT) |
| Parámetros | `fecha_corte` (vacío = último día del mes anterior), `modo` (`incremental` \| `reproceso`), `ambiente`, `catalog`, `schema`, `volume`, `run_id = {{job.run_id}}`, `job_id = {{job.id}}` |
| Concurrencia | `max_concurrent_runs = 1` (no hay corridas solapadas sobre el mismo corte) |
| Reintentos | Bronze y Gold: 2 cada 5 min. Silver: 1, porque un fallo por regla BLOQUEO es determinístico y reintentarlo no lo corrige. Validar: 1. Gobierno: 2 |
| Timeouts | 30/45/30/20/15 min por tarea; 3 h para el job completo |
| Alertas | Correo *on failure* y *duration warning* (regla de salud > 1 h). Corporativo: webhook a Teams o PagerDuty |
| Bitácora | Cada tarea registra RUNNING, SUCCESS o FAILED con duración, volúmenes y mensaje de error en `ctl_ejecuciones` |

## 2. Reejecución, idempotencia y recuperación
- **Reejecutar el mismo corte** no duplica:
  - los archivos ya ingeridos se omiten por hash;
  - Silver hace MERGE por clave;
  - Gold hace `replaceWhere fecha_corte`;
  - la cuarentena hace MERGE por (`dataset`, `_record_id`, `regla_id`);
  - el control hace MERGE por (`run_id`, `tarea`).
- **Recuperación desde fallos.** Se usa *Repair run* del job, que reejecuta solo la tarea fallida y las siguientes. Cada tarea lee lo persistido por la anterior; no depende del estado en memoria.
- **Backfill de un periodo.** Se ejecuta *Run now with different parameters* con `fecha_corte = AAAA-MM-DD` y `modo = reproceso`. El modo reproceso reingiere los archivos, reemplazando sus filas por hash, y recalcula y reemplaza solo la foto Gold de ese corte. Varios meses se procesan secuencialmente, del más antiguo al más reciente.
- **Archivo defectuoso.** Su estado en `ctl_archivos` pasa a `RECHAZADO` (UPDATE con motivo) y se reejecuta. Silver ignora los archivos rechazados.
- **Archivo que incumple el contrato.** Queda en `PENDIENTE_REVISION` y bloquea hasta que el operador decida:
  - `RECHAZADO`: se excluye y se solicita el reenvío al productor;
  - `APROBADO_MANUAL`: se acepta tal como llegó.

  Un reintento automático **no** salta este bloqueo.
- **Rollback de datos:** `RESTORE TABLE <tabla> TO VERSION AS OF <n>` (time travel), o reproceso del corte.

## 3. Acuerdos operativos propuestos

| Acuerdo | Valor |
|---|---|
| Entrega de archivos por los productores (contrato) | A más tardar el día 4, 23:59 COT |
| Disponibilidad del corte certificado (SLA) | Día 5, 09:00 COT (3 h de ventana con reintentos) |
| Frescura máxima del dato de negocio | ≤ 35 días respecto al corte |
| Atención de incidentes P1 (sin publicación / dato erróneo publicado) | Respuesta 30 min hábiles, solución o *workaround* 4 h |
| Atención P2 (advertencias de calidad, discrepancias altas) | Respuesta 1 día hábil |
| Disponibilidad mensual del producto | 99% en horario hábil |

## 4. Observabilidad: tablero y tabla de control
Vistas en `resources/sql/03_observabilidad.sql`; tablero en `notebooks/07_observabilidad_y_consultas_negocio`.

| Indicador | Fuente |
|---|---|
| Estado de ejecución y duración por tarea | `vw_obs_ejecuciones` |
| Volumen de entrada y salida, aceptados y rechazados, % de rechazo | `vw_obs_volumenes` |
| Duplicados exactos y de negocio, versiones reemplazadas | `vw_obs_volumenes` |
| Frescura y cumplimiento del SLA | `vw_obs_frescura_sla` |
| Evolución del esquema | `vw_obs_evolucion_esquema` |
| Resultado de reglas, reconciliación y postcondiciones | `vw_obs_calidad_resumen`, `gold_calidad_pipeline` |
| Costo o consumo estimado | `vw_obs_ejecuciones.dbu_estimados` / `costo_estimado_usd` (duración × DBU/h × USD/DBU, parametrizable). Corporativo: `system.billing.usage` por `job_id` |
| Última ejecución exitosa y archivos procesados | `vw_obs_ultima_ejecucion_exitosa` |

**AI/BI Dashboard (opcional, 5 minutos).** Ir a **Dashboards → Create dashboard → Data** y agregar como datasets las vistas `vw_obs_*` → crear los widgets:
- KPI: estado de la última ejecución y cumplimiento del SLA;
- barras: rechazados por dataset;
- línea: duración por ejecución;
- tabla: reglas en ADVERTENCIA o FALLA.

## 5. Alertas, responsables y procedimiento

| Alerta | Disparador | Responsable | Procedimiento |
|---|---|---|---|
| Job fallido | `on_failure` | Ingeniero de datos de turno | Revisar `ctl_ejecuciones.mensaje` y el log de la tarea. Si es transitorio: *Repair run*. Si es de datos: siguiente fila |
| Regla BLOQUEO | `CalidadBloqueanteError` | Data steward del dominio + ingeniero de turno | Consultar `gold_calidad_pipeline` y `silver_cuarentena`, contactar al productor y decidir rechazo o reenvío del archivo. Reprocesar |
| Contrato incumplido | `ContratoIncumplidoError` | Owner técnico del sistema fuente | Validar el cambio de esquema. Ajustar el contrato (nuevo alias o versión) vía PR, o rechazar el archivo |
| SLA en riesgo | Duración > 1 h o sin publicación a las 09:00 | Líder del equipo de datos | Comunicar a los consumidores; las vistas certificadas siguen mostrando el último corte válido |
| Advertencias de calidad | Resultado ADVERTENCIA | Data steward | Revisión semanal de tendencias y ajuste de umbrales en el contrato |

**Postmortem:** todo incidente P1 se documenta con causa raíz, impacto, corrección y acción preventiva (nueva regla o prueba).
