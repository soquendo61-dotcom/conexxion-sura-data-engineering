# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta Bronze
# MAGIC Lee los archivos `<dataset>*.csv` del volumen, calcula el hash SHA-256 de cada archivo, omite los ya ingeridos (idempotencia), agrega metadatos de ingesta (archivo, hash, run_id, fecha de corte, hash de fila), valida el data contract y escribe `bronze_<dataset>` con `replaceWhere` por hash de archivo.
# MAGIC
# MAGIC **Parámetros** (widgets / parámetros del job): `fecha_corte`, `modo`, `ambiente`, `run_id`, `catalog`, `schema`, `volume`.
# MAGIC Toda la lógica vive en `src/` (módulos reutilizables y probados); este notebook solo orquesta.

# COMMAND ----------

# DBTITLE 1,Importar módulos del repositorio
import os
import sys

RAIZ_REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if os.path.join(RAIZ_REPO, "src") not in sys.path:
    sys.path.insert(0, os.path.join(RAIZ_REPO, "src"))

from pipeline import tasks  # noqa: E402
from common.logging_utils import get_logger  # noqa: E402
from pipeline.notebook_params import obtener_config  # noqa: E402

cfg = obtener_config(dbutils)
get_logger("notebook", cfg.run_id).info(f"Parámetros: {cfg.as_dict()}")

# COMMAND ----------

# DBTITLE 1,Ejecutar tarea con bitácora (ctl_ejecuciones)
tasks.tarea_preparar(spark, cfg)
resultado = tasks.ejecutar_tarea(spark, cfg, "bronze", lambda: tasks.tarea_bronze(spark, cfg))
resultado
