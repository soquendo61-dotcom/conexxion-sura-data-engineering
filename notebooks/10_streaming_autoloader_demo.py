# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · (Opcional) Extensión casi en tiempo real para movimientos – prueba de concepto
# MAGIC Diseño completo en `docs/08_opcionales_streaming_ia.md`. Esta PoC usa **Auto Loader** sobre una carpeta del volumen
# MAGIC con `trigger(availableNow=True)` (Free Edition / serverless no permite streams continuos), lo que equivale a
# MAGIC micro-batches programables cada pocos minutos con exactamente-una-vez gracias al checkpoint.
# MAGIC
# MAGIC - **Fuente/contrato:** archivos de eventos `movimientos*.csv` en `raw_eventos/` (mismo contrato que `movimientos`).
# MAGIC - **Garantía:** checkpoint en el volumen → cada archivo se procesa una sola vez; sink Delta idempotente.
# MAGIC - **Duplicados / tardíos / fuera de orden:** `MERGE` por `evento_id` en `foreachBatch`; el orden se resuelve con
# MAGIC   (`fecha_evento`, `secuencia`, `fecha_ingesta`), no con el orden de llegada.
# MAGIC - **Dead-letter:** eventos sin `evento_id`/`id_entidad` o con payload inválido van a `stream_movimientos_dlq`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("volume", "volume1")
c, s, v = (dbutils.widgets.get(k) for k in ("catalog", "schema", "volume"))
ruta_eventos = f"/Volumes/{c}/{s}/{v}/raw_eventos"
ruta_checkpoint = f"/Volumes/{c}/{s}/{v}/_checkpoints/stream_movimientos"
dbutils.fs.mkdirs(ruta_eventos)
# Para la demo se copia el archivo de movimientos como si llegara al buzón de eventos.
dbutils.fs.cp(f"/Volumes/{c}/{s}/{v}/raw/movimientos.csv", f"{ruta_eventos}/movimientos_lote_001.csv")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import Window

esquema_payload = "estado_origen string, estado_destino string, usuario_proceso string"
destino = f"{c}.{s}.stream_movimientos_silver"
dlq = f"{c}.{s}.stream_movimientos_dlq"

eventos = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{ruta_checkpoint}/schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")      # columnas inesperadas -> _rescued_data
    .option("header", "true").option("sep", ";").option("quote", '"').option("escape", '"')
    .load(ruta_eventos)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_payload", F.from_json("payload", esquema_payload))
)


def procesar_lote(df, batch_id):
    """foreachBatch: separa DLQ y aplica MERGE idempotente (duplicados y eventos fuera de orden)."""
    invalido = F.col("evento_id").isNull() | F.col("id_entidad").isNull() | \
        (F.col("payload").isNotNull() & F.col("_payload").isNull())
    df.filter(invalido).withColumn("_batch_id", F.lit(batch_id)) \
      .write.mode("append").saveAsTable(dlq)
    validos = df.filter(~invalido)
    # Deduplicar dentro del micro-batch: el evento más reciente por evento_id.
    w = Window.partitionBy("evento_id").orderBy(F.col("fecha_ingesta").desc(), F.col("secuencia").cast("int").desc())
    ultimo = validos.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_payload")
    if not spark.catalog.tableExists(destino):
        ultimo.limit(0).write.saveAsTable(destino)
    (DeltaTable.forName(spark, destino).alias("t")
        .merge(ultimo.alias("s"), "t.evento_id = s.evento_id")
        .whenMatchedUpdateAll(condition="s.fecha_ingesta > t.fecha_ingesta")   # llegó una versión más nueva
        .whenNotMatchedInsertAll()
        .execute())


consulta = (
    eventos.writeStream.foreachBatch(procesar_lote)
    .option("checkpointLocation", ruta_checkpoint)
    .trigger(availableNow=True)
    .start()
)
consulta.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Evidencia: exactamente una vez (re-ejecutar la celda anterior no cambia los conteos)
display(spark.sql(f"SELECT count(*) AS eventos, count(DISTINCT evento_id) AS eventos_unicos FROM {destino}"))
display(spark.sql(f"SELECT count(*) AS en_dlq FROM {dlq}") if spark.catalog.tableExists(dlq) else spark.sql("SELECT 0 AS en_dlq"))
