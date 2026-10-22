# Databricks notebook source
# MAGIC %md
# MAGIC # Silver + calidad + cuarentena
# MAGIC Estandariza tipos y fechas, deduplica (exactos, versiones, duplicados de negocio), valida integridad referencial, evalúa las reglas de calidad (severidad y umbral), envía rechazos a `silver_cuarentena`, reconcilia Bronze vs Silver y sincroniza `silver_<dataset>` con MERGE. Falla si una regla BLOQUEO supera su umbral (Gold no se publica).
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
resultado = tasks.ejecutar_tarea(spark, cfg, "silver", lambda: tasks.tarea_silver(spark, cfg))
resultado
