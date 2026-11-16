# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Trazabilidad de un registro y de un lote
# MAGIC Recorre un registro desde el producto Gold hasta el archivo fuente y la ejecución que lo cargó:
# MAGIC **Gold → Silver → Bronze → archivo (`ctl_archivos`) → ejecución (`ctl_ejecuciones`)**, más la cuarentena y el
# MAGIC historial Delta (time travel). Por defecto usa `POL0000101`, que llegó duplicado en la fuente.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("poliza_id", "POL0000101", "Póliza a trazar")
esquema = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
poliza = dbutils.widgets.get("poliza_id").strip()

# COMMAND ----------

# DBTITLE 1,1. Producto Gold (último corte) con su run_id
display(spark.sql(f"""
  SELECT fecha_corte, poliza_id, estado_corte, prima_emitida, recaudo_aplicado, costo_incurrido,
         ultima_actividad_fecha, _run_id AS run_id_gold, _gold_ts
  FROM {esquema}.gold_poliza_360 WHERE poliza_id = '{poliza}' ORDER BY fecha_corte DESC"""))

# COMMAND ----------

# DBTITLE 1,2. Silver: versión vigente y linaje hacia Bronze
display(spark.sql(f"""
  SELECT poliza_id, estado, prima, fecha_actualizacion, _dq_advertencias, _record_id, _source_file,
         _file_hash, _bronze_run_id, _silver_run_id, _silver_ts
  FROM {esquema}.silver_polizas WHERE poliza_id = '{poliza}'"""))

# COMMAND ----------

# DBTITLE 1,3. Bronze: todas las filas recibidas para la clave (incluye duplicados)
display(spark.sql(f"""
  SELECT poliza_id, estado, prima, fecha_actualizacion, _file_name, _file_hash, _ingestion_ts, _run_id,
         _fecha_corte, _row_hash, _record_id
  FROM {esquema}.bronze_polizas WHERE poliza_id = '{poliza}' ORDER BY _ingestion_ts"""))

# COMMAND ----------

# DBTITLE 1,4. Cuarentena: registros de esta clave rechazados y su causa
display(spark.sql(f"""
  SELECT dataset, clave_negocio, regla_id, severidad, descripcion_regla, estado_tratamiento,
         run_id, run_id_ultima_deteccion, _source_file, registro_original
  FROM {esquema}.silver_cuarentena WHERE clave_negocio = '{poliza}'"""))

# COMMAND ----------

# DBTITLE 1,5. Lote: archivo de origen y ejecución que lo ingirió
display(spark.sql(f"""
  SELECT a.dataset, a.file_name, a.file_hash, a.registros, a.estado, a.columnas_nuevas,
         e.run_id, e.tarea, e.estado AS estado_ejecucion, e.inicio, e.fin, e.usuario
  FROM {esquema}.ctl_archivos a
  JOIN (SELECT DISTINCT _file_hash FROM {esquema}.bronze_polizas WHERE poliza_id = '{poliza}') b
    ON a.file_hash = b._file_hash
  LEFT JOIN {esquema}.ctl_ejecuciones e ON e.run_id = a.run_id AND e.tarea = 'bronze'"""))

# COMMAND ----------

# DBTITLE 1,6. Auditoría Delta: historial y time travel
display(spark.sql(f"DESCRIBE HISTORY {esquema}.gold_poliza_360"))
version_inicial = spark.sql(f"DESCRIBE HISTORY {esquema}.gold_poliza_360").agg({"version": "min"}).first()[0]
display(spark.sql(f"""
  SELECT fecha_corte, count(*) AS polizas FROM {esquema}.gold_poliza_360 VERSION AS OF {version_inicial}
  GROUP BY fecha_corte"""))

# COMMAND ----------

# DBTITLE 1,7. Linaje en Unity Catalog
# MAGIC %md
# MAGIC Catalog Explorer → `gold_poliza_360` → pestaña **Lineage** muestra el grafo tabla/columna generado
# MAGIC automáticamente por Unity Catalog (Bronze → Silver → Gold y notebooks que lo producen).
