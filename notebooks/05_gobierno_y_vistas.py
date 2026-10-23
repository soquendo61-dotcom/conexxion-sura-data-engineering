# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gobierno Unity Catalog y vistas de consumo / observabilidad
# MAGIC - Vistas certificadas (`vw_gold_*_certificada`): solo el último corte **PUBLICADO**.
# MAGIC - Vistas de observabilidad (`vw_obs_*`): base del tablero de control.
# MAGIC - Gobierno: funciones de enmascaramiento, máscaras de columna PII, tags de clasificación,
# MAGIC   comentarios y permisos por rol (mínimo privilegio). Las sentencias que Free Edition no
# MAGIC   soporte (p. ej. grupos de cuenta inexistentes) se reportan como `NO_APLICADA`.

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
dbutils.widgets.text("evaluador_1", "yesidmontoya@sura.com.co", "Evaluador 1")
dbutils.widgets.text("evaluador_2", "fjcarvajal@sura.com.co", "Evaluador 2")
dbutils.widgets.text("grupo_pii", "pii_lectores", "Grupo lectores PII")
dbutils.widgets.text("grupo_ingenieros", "data_engineers", "Grupo ingenieros")
dbutils.widgets.text("grupo_analistas", "data_analysts", "Grupo analistas")
dbutils.widgets.text("grupo_stewards", "data_stewards", "Grupo data stewards")
variables = {k: dbutils.widgets.get(k) for k in
             ("evaluador_1", "evaluador_2", "grupo_pii", "grupo_ingenieros", "grupo_analistas", "grupo_stewards")}

# COMMAND ----------

# DBTITLE 1,Aplicar vistas y gobierno (idempotente)
resultado = {}


def _ejecutar():
    r = tasks.tarea_gobierno_y_vistas(spark, cfg, os.path.join(RAIZ_REPO, "resources", "sql"), variables)
    resultado.update(r)
    return {"mensaje": r["mensaje"]}


tasks.tarea_preparar(spark, cfg)
tasks.ejecutar_tarea(spark, cfg, "gobierno_y_vistas", _ejecutar)
display(spark.createDataFrame(resultado["detalle"], "sentencia string, estado string, detalle string"))

# COMMAND ----------

# DBTITLE 1,Evidencia: objetos registrados en Unity Catalog
display(spark.sql(f"SHOW TABLES IN {cfg.catalog}.{cfg.schema}"))
display(spark.sql(f"SHOW USER FUNCTIONS IN {cfg.catalog}.{cfg.schema}"))
display(spark.sql(f"SHOW VOLUMES IN {cfg.catalog}.{cfg.schema}"))

# COMMAND ----------

# DBTITLE 1,Evidencia: permisos y máscara de PII
display(spark.sql(f"SHOW GRANTS ON SCHEMA {cfg.catalog}.{cfg.schema}"))
display(spark.sql(f"SELECT cliente_id, numero_documento, correo, telefono, fecha_nacimiento "
                  f"FROM {cfg.tabla('silver_clientes')} LIMIT 5"))
