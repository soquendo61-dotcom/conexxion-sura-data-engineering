# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Tablero de control (observabilidad) y preguntas de negocio
# MAGIC Este notebook funciona como tablero mínimo. Las mismas vistas (`vw_obs_*`) pueden publicarse
# MAGIC como **AI/BI Dashboard** (Dashboards → Create → agregar dataset con cada vista), ver `docs/06_operacion_observabilidad.md`.

# COMMAND ----------

# DBTITLE 1,Parámetros
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
esquema = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

# COMMAND ----------

# MAGIC %md ## 1. Observabilidad técnica y de datos

# COMMAND ----------

# DBTITLE 1,Estado, duración, volumen y costo estimado por ejecución
display(spark.sql(f"""
  SELECT run_id, tarea, estado, fecha_corte, modo, inicio_cot, fin_cot, duracion_s,
         registros_entrada, registros_salida, registros_aceptados, registros_rechazados,
         dbu_estimados, costo_estimado_usd, mensaje
  FROM {esquema}.vw_obs_ejecuciones ORDER BY inicio_cot DESC LIMIT 50"""))

# COMMAND ----------

# DBTITLE 1,Volúmenes, aceptados, rechazados y duplicados por dataset (última ejecución)
display(spark.sql(f"""
  SELECT * FROM {esquema}.vw_obs_volumenes
  WHERE run_id = (SELECT max_by(run_id, ts_registro_cot) FROM {esquema}.vw_obs_volumenes)
  ORDER BY capa, dataset"""))

# COMMAND ----------

# DBTITLE 1,Frescura y cumplimiento de SLA
display(spark.sql(f"SELECT * FROM {esquema}.vw_obs_frescura_sla ORDER BY fecha_corte DESC"))

# COMMAND ----------

# DBTITLE 1,Evolución del esquema de archivos recibidos
display(spark.sql(f"SELECT * FROM {esquema}.vw_obs_evolucion_esquema ORDER BY dataset, ingesta_cot"))

# COMMAND ----------

# DBTITLE 1,Resumen de calidad por ejecución y dataset
display(spark.sql(f"SELECT * FROM {esquema}.vw_obs_calidad_resumen ORDER BY run_id DESC, capa, dataset"))

# COMMAND ----------

# MAGIC %md ## 2. Preguntas de negocio (sección 4.1)

# COMMAND ----------

# DBTITLE 1,P1. Pólizas vigentes, vencidas, canceladas o próximas a vencer al corte
display(spark.sql(f"""
  SELECT fecha_corte, estado_corte, count(*) AS polizas, sum(prima_emitida) AS prima_emitida
  FROM {esquema}.vw_gold_poliza_360_certificada
  GROUP BY fecha_corte, estado_corte ORDER BY polizas DESC"""))

# COMMAND ----------

# DBTITLE 1,P2. Prima emitida, recaudada y pendiente por producto, sucursal y mes
display(spark.sql(f"""
  SELECT periodo, producto, sucursal, prima_emitida, prima_recaudada, recaudo_pendiente, prima_pendiente_acumulada
  FROM {esquema}.vw_gold_siniestralidad_certificada
  ORDER BY periodo DESC, producto, sucursal"""))

# COMMAND ----------

# DBTITLE 1,P3. Costo incurrido y razón de siniestralidad mensual
display(spark.sql(f"""
  SELECT periodo, sum(prima_devengada) AS prima_devengada, sum(costo_incurrido) AS costo_incurrido,
         round(sum(costo_incurrido) / nullif(sum(prima_devengada), 0), 4) AS razon_siniestralidad
  FROM {esquema}.vw_gold_siniestralidad_certificada
  GROUP BY periodo ORDER BY periodo"""))

# COMMAND ----------

# DBTITLE 1,P4. Pagos sin correspondencia, siniestros sin póliza válida, movimientos fuera de vigencia
display(spark.sql(f"""
  SELECT tipo_discrepancia, severidad, count(*) AS casos, sum(valor) AS valor_total
  FROM {esquema}.gold_discrepancias
  WHERE fecha_corte = (SELECT max(fecha_corte) FROM {esquema}.ctl_publicaciones WHERE estado = 'PUBLICADO')
  GROUP BY tipo_discrepancia, severidad ORDER BY severidad, casos DESC"""))

# COMMAND ----------

# DBTITLE 1,P5. Reglas incumplidas y registros en cuarentena
display(spark.sql(f"""
  SELECT capa, dataset, tipo_control, regla_id, severidad, resultado, registros_fallidos, pct_fallo, umbral_pct
  FROM {esquema}.gold_calidad_pipeline
  WHERE run_id = (SELECT run_id FROM {esquema}.vw_obs_ultima_ejecucion_exitosa LIMIT 1)
    AND (registros_fallidos > 0 OR resultado <> 'OK')
  ORDER BY capa, dataset, regla_id"""))
display(spark.sql(f"""
  SELECT dataset, regla_id, severidad, estado_tratamiento, count(*) AS registros
  FROM {esquema}.silver_cuarentena
  GROUP BY dataset, regla_id, severidad, estado_tratamiento ORDER BY dataset, registros DESC"""))

# COMMAND ----------

# DBTITLE 1,P6. Última ejecución exitosa y datos procesados
display(spark.sql(f"SELECT * FROM {esquema}.vw_obs_ultima_ejecucion_exitosa"))
