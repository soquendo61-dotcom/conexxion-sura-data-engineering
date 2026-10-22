# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuración inicial (una sola vez)
# MAGIC 1. Crea el volumen gobernado `workspace.default.volume1` y la carpeta de aterrizaje `raw/`.
# MAGIC 2. Verifica que los 5 archivos fuente estén cargados en el volumen.
# MAGIC 3. Crea las tablas de control, calidad y cuarentena.
# MAGIC
# MAGIC **Antes de ejecutar la celda 3:** suba `clientes.csv`, `polizas.csv`, `recaudos.csv`, `siniestros.csv` y
# MAGIC `movimientos.csv` a `/Volumes/workspace/default/volume1/raw/`
# MAGIC (Catalog → workspace → default → volume1 → raw → *Upload to this volume*). Ver `docs/guia_databricks_github.md`.

# COMMAND ----------

# DBTITLE 1,Importar módulos del repositorio
import os
import sys

RAIZ_REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if os.path.join(RAIZ_REPO, "src") not in sys.path:
    sys.path.insert(0, os.path.join(RAIZ_REPO, "src"))

from common.logging_utils import get_logger  # noqa: E402
from contracts.definitions import ORDEN_PROCESAMIENTO  # noqa: E402
from pipeline import tasks  # noqa: E402
from pipeline.notebook_params import obtener_config  # noqa: E402

cfg = obtener_config(dbutils)
log = get_logger("configuracion_inicial", cfg.run_id)

# COMMAND ----------

# DBTITLE 1,Volumen gobernado por Unity Catalog y carpeta de aterrizaje
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema}.{cfg.volume} "
          "COMMENT 'Zona de aterrizaje de archivos fuente (CSV/Parquet) de la prueba'")
dbutils.fs.mkdirs(cfg.ruta_raw)
log.info(f"Zona de aterrizaje: {cfg.ruta_raw}")

# COMMAND ----------

# DBTITLE 1,Verificar archivos fuente cargados
archivos = [f.name for f in dbutils.fs.ls(cfg.ruta_raw)]
faltantes = [ds for ds in ORDEN_PROCESAMIENTO if not any(a.startswith(ds) for a in archivos)]
display(spark.createDataFrame([(a,) for a in archivos] or [("(vacío)",)], "archivo string"))
if faltantes:
    raise FileNotFoundError(f"Faltan archivos en {cfg.ruta_raw} para: {faltantes}. Súbalos y reejecute.")
log.info("Los 5 datasets fuente están disponibles.")

# COMMAND ----------

# DBTITLE 1,Tablas de control, calidad y cuarentena
tasks.tarea_preparar(spark, cfg)
display(spark.sql(f"SHOW TABLES IN {cfg.catalog}.{cfg.schema}"))
