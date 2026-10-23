"""Tablas de control operativo: ejecuciones, métricas, archivos, watermarks y publicaciones.

Estas tablas son la base de la observabilidad (tablero), la idempotencia de la ingesta y la
trazabilidad "registro -> archivo -> ejecución".
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from common.config import PipelineConfig
from common.spark_utils import merge_por_clave

CTL_EJECUCIONES = "ctl_ejecuciones"
CTL_METRICAS = "ctl_metricas_dataset"
CTL_ARCHIVOS = "ctl_archivos"
CTL_WATERMARKS = "ctl_watermarks"
CTL_PUBLICACIONES = "ctl_publicaciones"

ESTADOS_ARCHIVO_VALIDOS = ("INGERIDO", "APROBADO_MANUAL")

DDL = {
    CTL_EJECUCIONES: """
        run_id STRING, job_id STRING, tarea STRING, estado STRING, fecha_corte DATE, modo STRING,
        ambiente STRING, inicio TIMESTAMP, fin TIMESTAMP, duracion_s DOUBLE,
        registros_entrada BIGINT, registros_salida BIGINT, registros_aceptados BIGINT,
        registros_rechazados BIGINT, mensaje STRING, parametros STRING, usuario STRING""",
    CTL_METRICAS: """
        run_id STRING, tarea STRING, capa STRING, dataset STRING, fecha_corte DATE,
        archivos_procesados BIGINT, registros_entrada BIGINT, registros_salida BIGINT,
        registros_aceptados BIGINT, registros_rechazados BIGINT, duplicados_exactos BIGINT,
        duplicados_negocio BIGINT, versiones_reemplazadas BIGINT, columnas_nuevas STRING,
        ts_registro TIMESTAMP""",
    CTL_ARCHIVOS: """
        dataset STRING, file_hash STRING, file_path STRING, file_name STRING, file_size BIGINT,
        file_modification_ts TIMESTAMP, registros BIGINT, columnas STRING, schema_hash STRING,
        columnas_nuevas STRING, columnas_faltantes STRING, estado STRING, motivo_estado STRING,
        run_id STRING, fecha_corte DATE, ts_ingesta TIMESTAMP""",
    CTL_WATERMARKS: """
        dataset STRING, capa STRING, tipo_watermark STRING, valor_watermark STRING,
        run_id STRING, ts_actualizacion TIMESTAMP""",
    CTL_PUBLICACIONES: """
        fecha_corte DATE, run_id STRING, estado STRING, ts_publicacion TIMESTAMP,
        tablas_publicadas STRING, observaciones STRING""",
}

COMENTARIOS = {
    CTL_EJECUCIONES: "Bitacora de ejecuciones por tarea del pipeline (estado, duracion, volumenes)",
    CTL_METRICAS: "Metricas de volumen y calidad por dataset, capa y ejecucion",
    CTL_ARCHIVOS: "Registro de archivos ingeridos con hash de contenido (idempotencia y linaje)",
    CTL_WATERMARKS: "Marcas de agua por dataset y capa para cargas incrementales",
    CTL_PUBLICACIONES: "Fechas de corte publicadas/certificadas para consumo",
}


def ahora_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def crear_tablas_control(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Crea (si no existen) las tablas de control con esquema explícito."""
    for nombre, columnas in DDL.items():
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {cfg.tabla(nombre)} ({columnas}) "
            f"USING DELTA COMMENT '{COMENTARIOS[nombre]}'"
        )


def _schema(nombre: str) -> T.StructType:
    return T._parse_datatype_string(DDL[nombre].replace("\n", " ").strip())


def _df(spark: SparkSession, nombre: str, filas: list[dict]) -> DataFrame:
    schema = _schema(nombre)
    datos = [tuple(f.get(c.name) for c in schema.fields) for f in filas]
    return spark.createDataFrame(datos, schema)


def upsert_control(spark: SparkSession, cfg: PipelineConfig, nombre: str, filas: list[dict],
                   claves: list[str]) -> None:
    """Upsert idempotente de filas de control (reintentos de una tarea no duplican filas)."""
    if not filas:
        return
    merge_por_clave(_df(spark, nombre, filas), cfg.tabla(nombre), claves)


def registrar_ejecucion(spark: SparkSession, cfg: PipelineConfig, fila: dict) -> None:
    base = {
        "run_id": cfg.run_id, "job_id": cfg.job_id, "fecha_corte": cfg.fecha_corte,
        "modo": cfg.modo, "ambiente": cfg.ambiente,
    }
    upsert_control(spark, cfg, CTL_EJECUCIONES, [{**base, **fila}], ["run_id", "tarea"])


def registrar_metricas(spark: SparkSession, cfg: PipelineConfig, filas: list[dict]) -> None:
    base = {"run_id": cfg.run_id, "fecha_corte": cfg.fecha_corte, "ts_registro": ahora_utc()}
    upsert_control(spark, cfg, CTL_METRICAS, [{**base, **f} for f in filas],
                   ["run_id", "capa", "dataset"])


def actualizar_watermark(spark: SparkSession, cfg: PipelineConfig, dataset: str, capa: str,
                         tipo: str, valor: str | None) -> None:
    if valor is None:
        return
    upsert_control(spark, cfg, CTL_WATERMARKS, [{
        "dataset": dataset, "capa": capa, "tipo_watermark": tipo, "valor_watermark": valor,
        "run_id": cfg.run_id, "ts_actualizacion": ahora_utc(),
    }], ["dataset", "capa"])


def hashes_archivos_validos(spark: SparkSession, cfg: PipelineConfig, dataset: str) -> DataFrame:
    """Hashes de archivos habilitados para alimentar Silver (excluye rechazados / en revisión)."""
    estados = ", ".join(f"'{e}'" for e in ESTADOS_ARCHIVO_VALIDOS)
    return spark.sql(
        f"SELECT file_hash AS _file_hash FROM {cfg.tabla(CTL_ARCHIVOS)} "
        f"WHERE dataset = '{dataset}' AND estado IN ({estados})"
    )
