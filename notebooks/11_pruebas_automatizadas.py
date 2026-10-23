# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Pruebas automatizadas en Databricks
# MAGIC Ejecuta la suite `tests/` (pruebas unitarias de transformaciones reutilizables) con pytest sobre el
# MAGIC cómputo del workspace. Las mismas pruebas corren en GitHub Actions en cada Pull Request (`.github/workflows/ci.yml`).
# MAGIC Las pruebas de integración/postcondiciones sobre las tablas Silver y Gold se ejecutan en cada corrida del
# MAGIC pipeline (tarea `validar_publicar`) y quedan en `gold_calidad_pipeline` con `tipo_control = 'POSTCONDICION'`.

# COMMAND ----------

# MAGIC %pip install pytest==8.4.2 --quiet

# COMMAND ----------

import os
import sys

import pytest

RAIZ_REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, os.path.join(RAIZ_REPO, "src"))
sys.dont_write_bytecode = True  # el sistema de archivos del repo es de solo lectura para .pyc
codigo = pytest.main([os.path.join(RAIZ_REPO, "tests"), "-v", "-p", "no:cacheprovider", "--rootdir", RAIZ_REPO])
assert codigo == 0, f"Fallaron pruebas (código {codigo})"

# COMMAND ----------

# DBTITLE 1,Postcondiciones de la última ejecución (integración sobre tablas Silver/Gold)
display(spark.sql("""
  SELECT capa, dataset, tipo_control, regla_id, valor_esperado, valor_obtenido, resultado
  FROM workspace.default.gold_calidad_pipeline
  WHERE tipo_control IN ('POSTCONDICION', 'RECONCILIACION')
    AND run_id = (SELECT run_id FROM workspace.default.vw_obs_ultima_ejecucion_exitosa LIMIT 1)
  ORDER BY capa, regla_id"""))
