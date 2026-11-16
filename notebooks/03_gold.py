# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: productos de datos
# MAGIC Construye `gold_poliza_360`, `gold_siniestralidad_mensual` y `gold_discrepancias` para la fecha de corte y los escribe con `replaceWhere fecha_corte = ...` (reprocesar un periodo no duplica).
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
resultado = tasks.ejecutar_tarea(spark, cfg, "gold", lambda: tasks.tarea_gold(spark, cfg))
resultado
