-- =====================================================================================
-- Vistas de observabilidad tecnica y de datos (base del tablero de control).
-- Variables: ${catalog} ${schema} ${dbu_por_hora} ${usd_por_dbu}
-- =====================================================================================

-- Estado, duracion, volumenes y costo estimado por ejecucion y tarea.
CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_obs_ejecuciones
COMMENT 'Estado, duracion, volumen y costo estimado por ejecucion y tarea'
AS
SELECT run_id, job_id, tarea, estado, fecha_corte, modo, ambiente,
       from_utc_timestamp(inicio, 'America/Bogota') AS inicio_cot,
       from_utc_timestamp(fin, 'America/Bogota') AS fin_cot,
       duracion_s, registros_entrada, registros_salida, registros_aceptados, registros_rechazados,
       round(duracion_s / 3600 * ${dbu_por_hora}, 4) AS dbu_estimados,
       round(duracion_s / 3600 * ${dbu_por_hora} * ${usd_por_dbu}, 4) AS costo_estimado_usd,
       mensaje, usuario
FROM ${catalog}.${schema}.ctl_ejecuciones;

-- Volumen de entrada/salida, aceptados, rechazados y duplicados por dataset.
CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_obs_volumenes
COMMENT 'Volumenes, aceptados, rechazados y duplicados por dataset, capa y ejecucion'
AS
SELECT m.run_id, m.fecha_corte, m.capa, m.dataset, m.archivos_procesados, m.registros_entrada,
       m.registros_salida, m.registros_aceptados, m.registros_rechazados,
       round(100 * m.registros_rechazados / nullif(m.registros_entrada, 0), 3) AS pct_rechazo,
       m.duplicados_exactos, m.duplicados_negocio, m.versiones_reemplazadas, m.columnas_nuevas,
       from_utc_timestamp(m.ts_registro, 'America/Bogota') AS ts_registro_cot
FROM ${catalog}.${schema}.ctl_metricas_dataset m;

-- Resumen de calidad por ejecucion y dataset.
CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_obs_calidad_resumen
COMMENT 'Resumen de reglas OK / ADVERTENCIA / FALLA / INFO por ejecucion y dataset'
AS
SELECT run_id, fecha_corte, capa, dataset,
       count(*) AS controles,
       sum(CASE WHEN resultado = 'OK' THEN 1 ELSE 0 END) AS ok,
       sum(CASE WHEN resultado = 'ADVERTENCIA' THEN 1 ELSE 0 END) AS advertencias,
       sum(CASE WHEN resultado = 'FALLA' THEN 1 ELSE 0 END) AS fallas,
       sum(CASE WHEN resultado = 'INFO' THEN 1 ELSE 0 END) AS informativas,
       sum(CASE WHEN tipo_control = 'REGLA' THEN registros_fallidos ELSE 0 END) AS registros_con_incumplimiento
FROM ${catalog}.${schema}.gold_calidad_pipeline
GROUP BY run_id, fecha_corte, capa, dataset;

-- Frescura y cumplimiento de SLA de publicacion.
-- Acuerdo: corte del mes M publicado a mas tardar el dia 5 del mes M+1 a las 09:00 COT
-- (el job inicia 06:00 COT; 3 horas de ventana incluyendo reintentos).
CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_obs_frescura_sla
COMMENT 'Frescura de la publicacion y cumplimiento del SLA (dia 5, 09:00 COT)'
AS
SELECT fecha_corte, run_id, estado,
       from_utc_timestamp(ts_publicacion, 'America/Bogota') AS publicado_cot,
       make_timestamp(year(add_months(fecha_corte, 1)), month(add_months(fecha_corte, 1)), 5, 9, 0, 0) AS limite_sla_cot,
       round((unix_timestamp(from_utc_timestamp(ts_publicacion, 'America/Bogota'))
              - unix_timestamp(make_timestamp(year(add_months(fecha_corte, 1)), month(add_months(fecha_corte, 1)), 5, 9, 0, 0))) / 3600, 2)
         AS horas_vs_sla,
       from_utc_timestamp(ts_publicacion, 'America/Bogota')
         <= make_timestamp(year(add_months(fecha_corte, 1)), month(add_months(fecha_corte, 1)), 5, 9, 0, 0) AS sla_cumplido,
       datediff(current_date(), fecha_corte) AS dias_desde_corte
FROM ${catalog}.${schema}.ctl_publicaciones;

-- Evolucion de esquema por archivo recibido.
CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_obs_evolucion_esquema
COMMENT 'Cambios de esquema entre archivos recibidos por dataset'
AS
SELECT dataset, file_name, from_utc_timestamp(ts_ingesta, 'America/Bogota') AS ingesta_cot, estado,
       schema_hash, columnas, columnas_nuevas, columnas_faltantes,
       lag(schema_hash) OVER (PARTITION BY dataset ORDER BY ts_ingesta) AS schema_hash_anterior,
       coalesce(schema_hash <> lag(schema_hash) OVER (PARTITION BY dataset ORDER BY ts_ingesta), false) AS cambio_esquema
FROM ${catalog}.${schema}.ctl_archivos;

-- Ultima ejecucion exitosa y archivos procesados (pregunta de negocio 6).
CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_obs_ultima_ejecucion_exitosa
COMMENT 'Ultima ejecucion exitosa del pipeline y archivos que proceso'
AS
WITH ultima AS (
  SELECT run_id, fecha_corte, inicio, fin, duracion_s
  FROM ${catalog}.${schema}.ctl_ejecuciones
  WHERE tarea = 'PIPELINE' AND estado = 'SUCCESS'
  ORDER BY fin DESC LIMIT 1
)
SELECT u.run_id, u.fecha_corte, from_utc_timestamp(u.inicio, 'America/Bogota') AS inicio_cot,
       from_utc_timestamp(u.fin, 'America/Bogota') AS fin_cot, u.duracion_s,
       a.dataset, a.file_name, a.registros, a.estado AS estado_archivo, a.run_id AS run_id_ingesta
FROM ultima u
LEFT JOIN ${catalog}.${schema}.ctl_archivos a ON a.estado IN ('INGERIDO', 'APROBADO_MANUAL');
