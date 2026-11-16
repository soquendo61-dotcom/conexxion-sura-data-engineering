"""Tareas del pipeline (una por task del Workflow de Databricks).

Cada tarea:
- recibe la misma ``PipelineConfig`` (run_id compartido por todo el job);
- registra inicio/fin/estado/volúmenes en ``ctl_ejecuciones`` (también si falla);
- es idempotente: reejecutarla con los mismos parámetros deja el mismo estado.

Dependencias del Workflow:
    preparar -> bronze -> silver (+calidad) -> gold -> validar_y_publicar
"""

from __future__ import annotations

import time
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import control
from common.config import PipelineConfig
from common.control import ahora_utc
from common.logging_utils import get_logger
from common.spark_utils import escribir_delta, merge_por_clave, tabla_existe
from contracts.definitions import CONTRATOS, ORDEN_PROCESAMIENTO
from ingestion.bronze import ContratoIncumplidoError, ingerir_dataset, nombre_tabla_bronze
from quality import engine
from transformations import gold
from transformations.scd2 import construir_scd2
from transformations.silver import (
    clasificar_versiones,
    construir_silver,
    nombre_tabla_silver,
    preparar,
    reconciliar_bronze_silver,
)

TABLA_CALIDAD = "gold_calidad_pipeline"
TABLA_CUARENTENA = "silver_cuarentena"
TABLA_SCD2_CLIENTES = "silver_clientes_historia"
TABLAS_GOLD = ("gold_poliza_360", "gold_siniestralidad_mensual", "gold_discrepancias")

DDL_CALIDAD = """
    run_id STRING, fecha_corte DATE, ambiente STRING, pipeline STRING, capa STRING, dataset STRING,
    tipo_control STRING, regla_id STRING, dimension STRING, descripcion STRING, severidad STRING,
    accion STRING, registros_evaluados BIGINT, registros_fallidos BIGINT, pct_fallo DOUBLE,
    umbral_pct DOUBLE, valor_esperado STRING, valor_obtenido STRING, resultado STRING,
    ts_evaluacion TIMESTAMP"""
DDL_CUARENTENA = """
    dataset STRING, clave_negocio STRING, _record_id STRING, regla_id STRING, dimension STRING,
    severidad STRING, descripcion_regla STRING, registro_original STRING, _source_file STRING,
    _file_hash STRING, _row_hash STRING, run_id STRING, pipeline STRING, fecha_corte DATE,
    ts_cuarentena TIMESTAMP, estado_tratamiento STRING, run_id_ultima_deteccion STRING,
    ts_ultima_deteccion TIMESTAMP"""
FRESCURA_MAX_DIAS = 35
COLUMNA_FECHA_NEGOCIO = {
    "clientes": "fecha_actualizacion", "polizas": "fecha_actualizacion", "recaudos": "fecha_pago",
    "siniestros": "fecha_aviso", "movimientos": "fecha_evento",
}


# ============================================================================ infraestructura
def ejecutar_tarea(spark: SparkSession, cfg: PipelineConfig, tarea: str, fn: Callable[[], dict]) -> dict:
    """Envuelve una tarea con bitácora, medición de duración y manejo de errores con contexto."""
    log = get_logger(tarea, cfg.run_id)
    inicio = ahora_utc()
    t0 = time.time()
    usuario = _usuario(spark)
    base = {"tarea": tarea, "inicio": inicio, "parametros": str(cfg.as_dict()), "usuario": usuario}
    control.registrar_ejecucion(spark, cfg, {**base, "estado": "RUNNING"})
    log.info(f"Inicio tarea '{tarea}' | fecha_corte={cfg.fecha_corte_str} modo={cfg.modo} ambiente={cfg.ambiente}")
    try:
        metricas = fn() or {}
    except Exception as exc:
        mensaje = f"{type(exc).__name__}: {exc}"[:4000]
        control.registrar_ejecucion(spark, cfg, {
            **base, "estado": "FAILED", "fin": ahora_utc(), "duracion_s": round(time.time() - t0, 2),
            "mensaje": mensaje,
        })
        log.error(f"Tarea '{tarea}' FALLÓ: {mensaje}")
        raise
    control.registrar_ejecucion(spark, cfg, {
        **base, "estado": "SUCCESS", "fin": ahora_utc(), "duracion_s": round(time.time() - t0, 2),
        "mensaje": metricas.pop("mensaje", None), **metricas,
    })
    log.info(f"Fin tarea '{tarea}' OK en {round(time.time() - t0, 1)} s | {metricas}")
    return metricas


def _usuario(spark: SparkSession) -> str | None:
    try:
        return spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    except Exception:
        return None


def guardar_resultados_calidad(spark: SparkSession, cfg: PipelineConfig, capa: str, filas: list[dict]) -> None:
    if not filas:
        return
    base = {"run_id": cfg.run_id, "fecha_corte": cfg.fecha_corte, "ambiente": cfg.ambiente,
            "pipeline": cfg.pipeline, "capa": capa, "ts_evaluacion": ahora_utc()}
    from pyspark.sql import types as T

    schema = T._parse_datatype_string(DDL_CALIDAD.replace("\n", " ").strip())
    datos = [tuple({**base, **f}.get(c.name) for c in schema.fields) for f in filas]
    df = spark.createDataFrame(datos, schema)
    merge_por_clave(df, cfg.tabla(TABLA_CALIDAD), ["run_id", "capa", "dataset", "tipo_control", "regla_id"])


def guardar_cuarentena(spark: SparkSession, cfg: PipelineConfig, dataset: str, cuarentena: DataFrame) -> None:
    """MERGE idempotente: un registro+regla se inserta una vez; reejecuciones solo actualizan la
    última detección. Si un registro deja de fallar (corregido en la fuente) se marca RESUELTO."""
    from delta.tables import DeltaTable

    tabla = cfg.tabla(TABLA_CUARENTENA)
    df = cuarentena.withColumn("run_id_ultima_deteccion", F.lit(cfg.run_id)) \
                   .withColumn("ts_ultima_deteccion", F.current_timestamp())
    (
        DeltaTable.forName(spark, tabla).alias("t")
        .merge(df.alias("s"), "t.dataset = s.dataset AND t._record_id = s._record_id AND t.regla_id = s.regla_id")
        .whenMatchedUpdate(set={
            "run_id_ultima_deteccion": "s.run_id_ultima_deteccion",
            "ts_ultima_deteccion": "s.ts_ultima_deteccion",
            "estado_tratamiento": "CASE WHEN t.estado_tratamiento = 'RESUELTO' THEN 'PENDIENTE' "
                                  "ELSE t.estado_tratamiento END",
        })
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceUpdate(
            condition=f"t.dataset = '{dataset}' AND t.estado_tratamiento = 'PENDIENTE'",
            set={"estado_tratamiento": "'RESUELTO'", "ts_ultima_deteccion": "current_timestamp()"},
        )
        .execute()
    )


# ============================================================================ tareas
def tarea_preparar(spark: SparkSession, cfg: PipelineConfig) -> dict:
    """Crea (si no existen) tablas de control, calidad y cuarentena; valida la zona de aterrizaje."""
    control.crear_tablas_control(spark, cfg)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg.tabla(TABLA_CALIDAD)} ({DDL_CALIDAD}) USING DELTA "
              "COMMENT 'Resultados de calidad, reconciliacion y postcondiciones por ejecucion, dataset y regla'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg.tabla(TABLA_CUARENTENA)} ({DDL_CUARENTENA}) USING DELTA "
              "COMMENT 'Registros rechazados por reglas de calidad con causa y trazabilidad'")
    return {}


def tarea_bronze(spark: SparkSession, cfg: PipelineConfig) -> dict:
    resultados = [ingerir_dataset(spark, cfg, CONTRATOS[ds]) for ds in ORDEN_PROCESAMIENTO]
    filas_calidad, incumplimientos = [], []
    for r in resultados:
        control.actualizar_watermark(spark, cfg, r.dataset, "bronze", "file_modification_ts", r.watermark)
        filas_calidad.append({
            "dataset": r.dataset, "tipo_control": "CONTRATO", "regla_id": f"CONTRATO_ESQUEMA_{r.dataset.upper()}",
            "dimension": "validez", "descripcion": "Archivos con todas las columnas obligatorias del contrato",
            "severidad": "BLOQUEO", "registros_evaluados": r.archivos_procesados,
            "registros_fallidos": len(r.incumplimientos), "valor_esperado": "0 incumplimientos",
            "valor_obtenido": "; ".join(r.incumplimientos) or "0 incumplimientos",
            "resultado": "FALLA" if r.incumplimientos else "OK",
        })
        if r.columnas_nuevas:
            filas_calidad.append({
                "dataset": r.dataset, "tipo_control": "CONTRATO", "regla_id": f"EVOLUCION_ESQUEMA_{r.dataset.upper()}",
                "dimension": "validez", "descripcion": "Columnas nuevas no contratadas (quedan en Bronze, no pasan a Silver)",
                "severidad": "ADVERTENCIA", "registros_evaluados": r.archivos_procesados,
                "registros_fallidos": len(r.columnas_nuevas), "valor_esperado": "sin columnas nuevas",
                "valor_obtenido": ",".join(r.columnas_nuevas), "resultado": "ADVERTENCIA",
            })
        incumplimientos += r.incumplimientos
    control.registrar_metricas(spark, cfg, [{
        "tarea": "bronze", "capa": "bronze", "dataset": r.dataset, "archivos_procesados": r.archivos_procesados,
        "registros_entrada": r.registros, "registros_salida": r.registros,
        "columnas_nuevas": ",".join(r.columnas_nuevas) or None,
    } for r in resultados])
    guardar_resultados_calidad(spark, cfg, "bronze", filas_calidad)
    if incumplimientos:
        raise ContratoIncumplidoError(
            "Incumplimiento de data contract. Revise ctl_archivos (estado PENDIENTE_REVISION) y "
            f"coordine con el productor: {incumplimientos}"
        )
    total = sum(r.registros for r in resultados)
    return {"registros_entrada": total, "registros_salida": total,
            "mensaje": f"archivos procesados={sum(r.archivos_procesados for r in resultados)}, "
                       f"omitidos por idempotencia={sum(r.archivos_omitidos for r in resultados)}"}


def tarea_silver(spark: SparkSession, cfg: PipelineConfig) -> dict:
    log = get_logger("silver", cfg.run_id)
    silver: dict[str, DataFrame] = {}
    filas_calidad: list[dict] = []
    metricas: list[dict] = []
    for ds in ORDEN_PROCESAMIENTO:
        contrato = CONTRATOS[ds]
        tabla_bronze = cfg.tabla(nombre_tabla_bronze(ds))
        if not tabla_existe(spark, tabla_bronze):
            raise RuntimeError(f"No existe {tabla_bronze}. Ejecute primero la tarea bronze.")
        bronze = spark.table(tabla_bronze).join(
            control.hashes_archivos_validos(spark, cfg, ds), "_file_hash", "left_semi"
        )
        salida = construir_silver(bronze, ds, silver, cfg)
        tabla = cfg.tabla(nombre_tabla_silver(ds))
        merge_por_clave(
            salida.aceptados, tabla, [contrato.clave],
            condicion_actualizacion="t._silver_hash <> s._silver_hash",
            borrar_no_presentes=True,
            propiedades={"delta.enableChangeDataFeed": "true", "capa": "silver"},
            comentario=f"Silver {ds}: {contrato.descripcion} Estandarizado, deduplicado y validado.",
        )
        guardar_cuarentena(spark, cfg, ds, salida.cuarentena)
        silver[ds] = spark.table(tabla)

        resultados = engine.resumir(salida.evaluado, salida.reglas)
        recon = reconciliar_bronze_silver(bronze, salida, ds)
        persistidos = silver[ds].count()
        recon.append(gold._control(
            "RECON_SILVER_PERSISTIDO", "Registros aceptados = registros persistidos en la tabla Silver",
            salida.metricas["registros_aceptados"], persistidos,
            persistidos == salida.metricas["registros_aceptados"], ds,
        ))
        filas_calidad += resultados + recon
        cuenta = {r["regla_id"]: r["registros_fallidos"] for r in resultados}
        pref = resultados[0]["regla_id"].split("_")[0]
        metricas.append({
            "tarea": "silver", "capa": "silver", "dataset": ds,
            "registros_entrada": salida.metricas["registros_entrada"],
            "registros_salida": persistidos,
            "registros_aceptados": salida.metricas["registros_aceptados"],
            "registros_rechazados": salida.metricas["registros_rechazados"],
            "versiones_reemplazadas": salida.metricas["versiones_reemplazadas"],
            "duplicados_exactos": cuenta.get(f"{pref}_DUP_EXACTO", 0),
            "duplicados_negocio": cuenta.get(f"{pref}_DUP_NEGOCIO", 0),
        })
        log.info(f"[{ds}] Silver: {salida.metricas}")

        if ds == "clientes":
            versiones = clasificar_versiones(preparar(bronze, contrato), contrato).filter(
                ~F.col("_dup_exacto") & ~F.col("_dup_negocio") & F.col("_corrupt_record").isNull()
            ).join(silver[ds].select("cliente_id"), "cliente_id", "left_semi")
            atributos = [c for c in contrato.nombres if c not in ("fecha_actualizacion", "origen")]
            escribir_delta(
                construir_scd2(versiones, "cliente_id", "fecha_actualizacion", atributos)
                .withColumn("_silver_run_id", F.lit(cfg.run_id)),
                cfg.tabla(TABLA_SCD2_CLIENTES),
                comentario="Historia SCD2 de clientes (valido_desde / valido_hasta / es_actual)",
            )

    control.registrar_metricas(spark, cfg, metricas)
    guardar_resultados_calidad(spark, cfg, "silver", filas_calidad)
    fallas = engine.bloqueos(filas_calidad)
    if fallas:
        raise engine.CalidadBloqueanteError(
            "Reglas BLOQUEO por encima del umbral; Gold no se publicará. "
            f"Detalle en gold_calidad_pipeline/silver_cuarentena: {[(f['dataset'], f['regla_id'], f['valor_obtenido']) for f in fallas]}"
        )
    return {
        "registros_entrada": sum(m["registros_entrada"] for m in metricas),
        "registros_salida": sum(m["registros_salida"] for m in metricas),
        "registros_aceptados": sum(m["registros_aceptados"] for m in metricas),
        "registros_rechazados": sum(m["registros_rechazados"] for m in metricas),
    }


def _silver(spark: SparkSession, cfg: PipelineConfig) -> dict[str, DataFrame]:
    return {ds: spark.table(cfg.tabla(nombre_tabla_silver(ds))) for ds in ORDEN_PROCESAMIENTO}


def tarea_gold(spark: SparkSession, cfg: PipelineConfig) -> dict:
    silver = _silver(spark, cfg)
    filtro = f"fecha_corte = DATE'{cfg.fecha_corte_str}'"
    p360 = gold.construir_poliza_360(silver, cfg)
    escribir_delta(p360, cfg.tabla("gold_poliza_360"), replace_where=filtro, cluster_by=["fecha_corte"],
                   comentario="Vista 360 de la póliza al corte: estado, prima, recaudo, siniestros y última actividad")
    p360 = spark.table(cfg.tabla("gold_poliza_360")).where(filtro)
    escribir_delta(gold.construir_siniestralidad_mensual(silver, cfg), cfg.tabla("gold_siniestralidad_mensual"),
                   replace_where=filtro, cluster_by=["fecha_corte", "periodo"],
                   comentario="Siniestralidad mensual por producto y sucursal (exposición, devengo, costo incurrido)")
    escribir_delta(gold.construir_discrepancias(silver, p360, cfg), cfg.tabla("gold_discrepancias"),
                   replace_where=filtro, cluster_by=["fecha_corte", "tipo_discrepancia"],
                   comentario="Discrepancias entre recaudos, pólizas, siniestros y movimientos al corte")
    conteos = {t: spark.table(cfg.tabla(t)).where(filtro).count() for t in TABLAS_GOLD}
    control.registrar_metricas(spark, cfg, [
        {"tarea": "gold", "capa": "gold", "dataset": t, "registros_salida": n} for t, n in conteos.items()
    ])
    return {"registros_salida": sum(conteos.values()), "mensaje": str(conteos)}


def controles_frescura(silver: dict[str, DataFrame], cfg: PipelineConfig) -> list[dict]:
    filas = []
    for ds, col in COLUMNA_FECHA_NEGOCIO.items():
        maxima = silver[ds].where(F.col(col) <= F.lit(cfg.fecha_corte_str).cast("date")) \
                           .agg(F.max(col).alias("m")).collect()[0]["m"]
        dias = (cfg.fecha_corte - maxima).days if maxima else None
        ok = dias is not None and dias <= FRESCURA_MAX_DIAS
        filas.append({
            "dataset": ds, "tipo_control": "FRESCURA", "regla_id": f"FRESCURA_{ds.upper()}",
            "dimension": "oportunidad", "descripcion": f"Último dato de negocio ({col}) a máximo {FRESCURA_MAX_DIAS} días del corte",
            "severidad": "ADVERTENCIA", "registros_fallidos": 0 if ok else 1, "pct_fallo": 0.0 if ok else 100.0,
            "valor_esperado": f"<= {FRESCURA_MAX_DIAS} días", "valor_obtenido": f"{dias} días (máx {maxima})",
            "resultado": "OK" if ok else "ADVERTENCIA",
        })
    return filas


def tarea_validar_publicar(spark: SparkSession, cfg: PipelineConfig) -> dict:
    silver = _silver(spark, cfg)
    filtro = f"fecha_corte = DATE'{cfg.fecha_corte_str}'"
    p360 = spark.table(cfg.tabla("gold_poliza_360")).where(filtro)
    sm = spark.table(cfg.tabla("gold_siniestralidad_mensual")).where(filtro)
    filas = gold.reconciliar_silver_gold(silver, p360, sm, cfg) + gold.postcondiciones(p360, sm, cfg) \
        + controles_frescura(silver, cfg)
    guardar_resultados_calidad(spark, cfg, "gold", filas)
    fallas = engine.bloqueos(filas)
    if fallas:
        control.upsert_control(spark, cfg, control.CTL_PUBLICACIONES, [{
            "fecha_corte": cfg.fecha_corte, "run_id": cfg.run_id, "estado": "RECHAZADO",
            "ts_publicacion": ahora_utc(), "tablas_publicadas": None,
            "observaciones": str([f["regla_id"] for f in fallas]),
        }], ["fecha_corte"])
        raise engine.CalidadBloqueanteError(f"Validación Gold fallida; no se publica: {fallas}")

    control.upsert_control(spark, cfg, control.CTL_PUBLICACIONES, [{
        "fecha_corte": cfg.fecha_corte, "run_id": cfg.run_id, "estado": "PUBLICADO",
        "ts_publicacion": ahora_utc(), "tablas_publicadas": ",".join(TABLAS_GOLD),
        "observaciones": f"{sum(1 for f in filas if f['resultado'] == 'ADVERTENCIA')} advertencias",
    }], ["fecha_corte"])
    for t in TABLAS_GOLD:
        control.actualizar_watermark(spark, cfg, t, "gold", "fecha_corte", cfg.fecha_corte_str)
    # Fila resumen de la ejecución completa (para el tablero de SLA).
    inicio = spark.sql(
        f"SELECT min(inicio) AS i FROM {cfg.tabla(control.CTL_EJECUCIONES)} WHERE run_id = '{cfg.run_id}'"
    ).collect()[0]["i"]
    fin = ahora_utc()
    control.registrar_ejecucion(spark, cfg, {
        "tarea": "PIPELINE", "estado": "SUCCESS", "inicio": inicio, "fin": fin,
        "duracion_s": round((fin - inicio).total_seconds(), 2) if inicio else None,
        "mensaje": f"Publicado corte {cfg.fecha_corte_str}", "parametros": str(cfg.as_dict()),
        "usuario": _usuario(spark),
    })
    return {"mensaje": f"Corte {cfg.fecha_corte_str} publicado"}


def ejecutar_pipeline_completo(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Ejecución secuencial (uso interactivo). En producción cada tarea es un task del Workflow."""
    for nombre, fn in (("preparar", tarea_preparar), ("bronze", tarea_bronze), ("silver", tarea_silver),
                       ("gold", tarea_gold), ("validar_publicar", tarea_validar_publicar)):
        if nombre == "preparar":
            fn(spark, cfg)  # crea las tablas de control antes de poder registrar en ellas
            continue
        ejecutar_tarea(spark, cfg, nombre, lambda fn=fn: fn(spark, cfg))


def tarea_gobierno_y_vistas(spark: SparkSession, cfg: PipelineConfig, ruta_sql: str,
                            variables_gobierno: dict[str, str]) -> dict:
    """Registra vistas certificadas/observabilidad y aplica gobierno (máscaras, tags, grants).

    Las vistas son obligatorias (fallan si hay error). El gobierno se aplica tolerando
    sentencias no soportadas por el entorno (p. ej. grupos inexistentes en Free Edition) y
    devuelve el detalle para evidencia.
    """
    from pipeline.sql_runner import ejecutar_sql

    variables = {"catalog": cfg.catalog, "schema": cfg.schema, "volume": cfg.volume,
                 "dbu_por_hora": "2.0", "usd_por_dbu": "0.35", **variables_gobierno}
    ejecutar_sql(spark, f"{ruta_sql}/02_vistas_consumo.sql", variables)
    ejecutar_sql(spark, f"{ruta_sql}/03_observabilidad.sql", variables)
    detalle = ejecutar_sql(spark, f"{ruta_sql}/01_gobierno_unity_catalog.sql", variables, tolerar_errores=True)
    aplicadas = sum(1 for d in detalle if d["estado"] == "OK")
    return {"mensaje": f"gobierno: {aplicadas}/{len(detalle)} sentencias aplicadas", "detalle": detalle}
