# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Ejecución completa (interactiva)
# MAGIC Ejecuta en secuencia, con un mismo `run_id`: preparar → bronze → silver → gold → validar/publicar → gobierno y vistas.
# MAGIC En producción las mismas funciones corren como tasks independientes del Workflow
# MAGIC (`resources/jobs/pipeline_polizas_siniestros.job.yml`) con dependencias, reintentos y alertas.
# MAGIC
# MAGIC **Para la prueba:** escriba `2026-08-31` en el widget `fecha_corte` (último mes con datos).

# COMMAND ----------

# DBTITLE 1,Importar módulos del repositorio
import os
import sys

RAIZ_REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if os.path.join(RAIZ_REPO, "src") not in sys.path:
    sys.path.insert(0, os.path.join(RAIZ_REPO, "src"))

from pipeline import tasks  # noqa: E402
from pipeline.notebook_params import obtener_config  # noqa: E402

cfg = obtener_config(dbutils)
cfg.as_dict()

# COMMAND ----------

# DBTITLE 1,Pipeline end-to-end
tasks.ejecutar_pipeline_completo(spark, cfg)
variables = {"evaluador_1": "yesidmontoya@sura.com.co", "evaluador_2": "fjcarvajal@sura.com.co",
             "grupo_pii": "pii_lectores", "grupo_ingenieros": "data_engineers",
             "grupo_analistas": "data_analysts", "grupo_stewards": "data_stewards"}
tasks.ejecutar_tarea(spark, cfg, "gobierno_y_vistas", lambda: {
    "mensaje": tasks.tarea_gobierno_y_vistas(
        spark, cfg, os.path.join(RAIZ_REPO, "resources", "sql"), variables)["mensaje"]
})

# COMMAND ----------

# DBTITLE 1,Resultado de la ejecución
display(spark.sql(f"SELECT * FROM {cfg.tabla('vw_obs_ejecuciones')} WHERE run_id = '{cfg.run_id}' ORDER BY inicio_cot"))
display(spark.sql(f"SELECT * FROM {cfg.tabla('vw_obs_volumenes')} WHERE run_id = '{cfg.run_id}' ORDER BY capa, dataset"))
display(spark.sql(f"SELECT * FROM {cfg.tabla('vw_obs_calidad_resumen')} WHERE run_id = '{cfg.run_id}' ORDER BY capa, dataset"))
