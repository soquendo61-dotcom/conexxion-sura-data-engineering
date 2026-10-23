# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Evidencia: nueva entrega, cambio de esquema, cuarentena, idempotencia y manejo de errores
# MAGIC Ejecutar **después** de una corrida exitosa del corte `2026-08-31`. Simula la entrega del mes siguiente
# MAGIC (corte `2026-09-30`) y tres situaciones operativas:
# MAGIC
# MAGIC | Escenario | Qué se espera |
# MAGIC |---|---|
# MAGIC | A. Novedades de pólizas con **cambio de esquema** (`prima` → `valor_prima`, columna nueva), una actualización, un **empate de marca temporal**, una prima negativa y un reenvío | El alias se resuelve por contrato, la columna nueva queda en Bronze y se reporta, la prima negativa va a cuarentena, la póliza actualizada cambia en Silver |
# MAGIC | B. **Reejecución** con los mismos archivos | Archivos omitidos por hash, mismos conteos (idempotencia) |
# MAGIC | C. Lote de recaudos **100% inválido** | La regla BLOQUEO supera el umbral → la tarea falla con mensaje claro, se registra FAILED, Gold no se publica. Recuperación: se marca el archivo RECHAZADO y se reprocesa |
# MAGIC | D. Archivo que **incumple el contrato** (sin `poliza_id`) | Bronze lo conserva, lo deja `PENDIENTE_REVISION` y bloquea la ejecución hasta decisión del operador |

# COMMAND ----------

import os
import sys

RAIZ_REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if os.path.join(RAIZ_REPO, "src") not in sys.path:
    sys.path.insert(0, os.path.join(RAIZ_REPO, "src"))

from common.config import PipelineConfig  # noqa: E402
from pipeline import tasks  # noqa: E402

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("volume", "volume1")
base = {k: dbutils.widgets.get(k) for k in ("catalog", "schema", "volume")}
cfg = PipelineConfig.desde_parametros({**base, "fecha_corte": "2026-09-30"})
esquema = f"{cfg.catalog}.{cfg.schema}"
cfg.as_dict()

# COMMAND ----------

# MAGIC %md ## A. Nueva entrega con cambio de esquema y casos de calidad

# COMMAND ----------

contenido = "\n".join([
    "poliza_id;cliente_id;producto;sucursal;fecha_inicio_vigencia;fecha_fin_vigencia;estado;valor_prima;moneda;canal;fecha_actualizacion;origen;canal_detalle",
    # Actualización de una póliza existente: pasa a Cancelada con fecha posterior
    "POL0000001;CLI000305;Autos;Bogota Norte;2026-01-27;2027-01-27;Cancelada;13991560.54;COP;Alianza;2026-09-15;CORE_POLIZAS;Alianza bancaria",
    # Empate de marca temporal: dos versiones distintas de la misma póliza con la misma fecha
    "POL0000002;CLI000100;Salud;Cali;2025-12-06;2026-12-06;Vigente;6206493.45;COP;Directo;2026-09-20;CORE_POLIZAS;Oficina",
    "POL0000002;CLI000100;Salud;Cali;2025-12-06;2026-12-06;Cancelada;6206493.45;COP;Directo;2026-09-20;CORE_POLIZAS;Oficina",
    # Póliza nueva válida (formato de fecha dd/mm/yyyy)
    "POL0002001;CLI000010;Hogar;Medellin;01/09/2026;01/09/2027;Vigente;2500000.00;COP;Digital;2026-09-01;CORE_POLIZAS;App",
    # Prima negativa -> cuarentena
    "POL0002002;CLI000011;Vida;Costa;2026-09-05;2027-09-05;Vigente;-150000.00;COP;Asesor;2026-09-05;CORE_POLIZAS;Asesor",
    # Reenvío exacto de una fila ya recibida en la entrega anterior (no debe duplicar)
    "POL0000003;CLI000344;Vida;Eje Cafetero;2025-01-20;2026-01-21;Renovada;3571929.78;COP;Asesor;2025-04-08;CORE_POLIZAS;Asesor",
])
dbutils.fs.put(f"{cfg.ruta_raw}/polizas_2026_09_novedades.csv", contenido, overwrite=True)
tasks.ejecutar_pipeline_completo(spark, cfg)

# COMMAND ----------

# DBTITLE 1,Evidencia A: evolución de esquema, cuarentena y actualización
display(spark.sql(f"SELECT dataset, file_name, estado, columnas_nuevas, columnas_faltantes, registros FROM {esquema}.ctl_archivos WHERE dataset = 'polizas'"))
display(spark.sql(f"SELECT dataset, clave_negocio, regla_id, severidad, run_id FROM {esquema}.silver_cuarentena WHERE clave_negocio IN ('POL0002002')"))
display(spark.sql(f"SELECT poliza_id, estado, fecha_actualizacion, _dq_advertencias, _file_hash FROM {esquema}.silver_polizas WHERE poliza_id IN ('POL0000001','POL0000002','POL0002001','POL0000003')"))
display(spark.sql(f"SELECT regla_id, registros_fallidos, resultado, valor_obtenido FROM {esquema}.gold_calidad_pipeline WHERE run_id = '{cfg.run_id}' AND (regla_id LIKE 'EVOLUCION%' OR regla_id LIKE 'POL_EMPATE%' OR regla_id LIKE 'POL_PRIMA%')"))

# COMMAND ----------

# MAGIC %md ## B. Reejecución idempotente (mismos archivos, mismo corte)

# COMMAND ----------

conteos_antes = {t: spark.table(f"{esquema}.{t}").count() for t in ("bronze_polizas", "silver_polizas", "silver_cuarentena", "gold_poliza_360")}
cfg_b = PipelineConfig.desde_parametros({**base, "fecha_corte": "2026-09-30"})
tasks.ejecutar_pipeline_completo(spark, cfg_b)
conteos_despues = {t: spark.table(f"{esquema}.{t}").count() for t in conteos_antes}
display(spark.createDataFrame([(t, conteos_antes[t], conteos_despues[t], conteos_antes[t] == conteos_despues[t]) for t in conteos_antes],
                              "tabla string, antes long, despues long, idempotente boolean"))

# COMMAND ----------

# MAGIC %md ## C. Lote inválido que supera un umbral BLOQUEO

# COMMAND ----------

filas = ["recaudo_id;poliza_id;fecha_pago;valor;medio_pago;estado_transaccion;referencia_bancaria;fecha_ingesta;origen"]
filas += [f"REC9000{i:04d};POL0000010;2026-09-10;-{1000 + i}.00;PSE;Aplicado;RBX{i:06d};2026-09-11;PAGOS" for i in range(200)]
dbutils.fs.put(f"{cfg.ruta_raw}/recaudos_2026_09_lote_erroneo.csv", "\n".join(filas), overwrite=True)
cfg_c = PipelineConfig.desde_parametros({**base, "fecha_corte": "2026-09-30"})
try:
    tasks.ejecutar_pipeline_completo(spark, cfg_c)
except Exception as exc:  # evidencia del manejo de error
    print(f"La ejecución falló como se esperaba -> {type(exc).__name__}: {str(exc)[:500]}")
display(spark.sql(f"SELECT tarea, estado, mensaje FROM {esquema}.ctl_ejecuciones WHERE run_id = '{cfg_c.run_id}'"))
display(spark.sql(f"SELECT dataset, regla_id, registros_fallidos, pct_fallo, umbral_pct, resultado FROM {esquema}.gold_calidad_pipeline WHERE run_id = '{cfg_c.run_id}' AND resultado = 'FALLA'"))

# COMMAND ----------

# DBTITLE 1,Recuperación: el operador rechaza el archivo y se reprocesa
spark.sql(f"""UPDATE {esquema}.ctl_archivos SET estado = 'RECHAZADO',
              motivo_estado = 'Lote con valores negativos. Rechazado por operador; se solicita reenvío al productor'
              WHERE file_name = 'recaudos_2026_09_lote_erroneo.csv'""")
cfg_c2 = PipelineConfig.desde_parametros({**base, "fecha_corte": "2026-09-30"})
tasks.ejecutar_pipeline_completo(spark, cfg_c2)
display(spark.sql(f"SELECT tarea, estado, duracion_s, mensaje FROM {esquema}.ctl_ejecuciones WHERE run_id = '{cfg_c2.run_id}'"))

# COMMAND ----------

# MAGIC %md ## D. Archivo que incumple el data contract

# COMMAND ----------

dbutils.fs.put(f"{cfg.ruta_raw}/siniestros_2026_09_sin_poliza.csv",
               "siniestro_id;fecha_ocurrencia;fecha_aviso;estado;valor_pagado;valor_reserva;fecha_actualizacion\n"
               "SIN90000001;2026-09-01;2026-09-02;Abierto;0;1000000;2026-09-02", overwrite=True)
cfg_d = PipelineConfig.desde_parametros({**base, "fecha_corte": "2026-09-30"})
try:
    tasks.ejecutar_pipeline_completo(spark, cfg_d)
except Exception as exc:
    print(f"Bloqueado por contrato -> {type(exc).__name__}: {str(exc)[:400]}")
display(spark.sql(f"SELECT file_name, estado, motivo_estado FROM {esquema}.ctl_archivos WHERE dataset = 'siniestros'"))

# COMMAND ----------

# DBTITLE 1,Decisión del operador: rechazar el archivo y dejar el pipeline en estado sano
spark.sql(f"UPDATE {esquema}.ctl_archivos SET estado = 'RECHAZADO' WHERE file_name = 'siniestros_2026_09_sin_poliza.csv'")
cfg_final = PipelineConfig.desde_parametros({**base, "fecha_corte": "2026-09-30"})
tasks.ejecutar_pipeline_completo(spark, cfg_final)
display(spark.sql(f"SELECT * FROM {esquema}.vw_obs_ejecuciones WHERE tarea = 'PIPELINE' ORDER BY inicio_cot DESC"))
