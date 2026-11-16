"""Ingesta batch de archivos del volumen a Bronze.

Principios:
- Copia fiel: todas las columnas se guardan como texto, sin alterar su significado.
- Metadatos de ingesta: archivo origen, hash del archivo, fecha/hora de ingesta, run_id,
  fecha de corte, hash de fila e identificador de registro.
- Idempotencia: cada archivo se identifica por el hash SHA-256 de su contenido. Un archivo ya
  ingerido se omite (modo incremental); en modo reproceso se reemplazan atómicamente sus filas
  (``replaceWhere _file_hash IN (...)``), nunca se duplican.
- Evolución de esquema: cada archivo se lee con su propio encabezado y se une por nombre;
  columnas nuevas se agregan a Bronze (``mergeSchema``) y se registran en ``ctl_archivos``.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.config import PipelineConfig
from common.control import CTL_ARCHIVOS, ahora_utc, upsert_control
from common.logging_utils import get_logger
from common.spark_utils import escribir_delta, tabla_existe
from contracts.definitions import ContratoDataset
from transformations.standardize import normalizar_columnas, validar_contrato

COLUMNA_CORRUPTA = "_corrupt_record"
OPCIONES_CSV = {
    "header": "true",
    "sep": ";",
    "quote": '"',
    "escape": '"',
    "encoding": "UTF-8",
    "mode": "PERMISSIVE",
    "multiLine": "false",
}


class ContratoIncumplidoError(RuntimeError):
    """Un archivo no trae columnas obligatorias del contrato: se bloquea la publicación."""


@dataclass
class ArchivoFuente:
    path: str
    nombre: str
    tamano: int
    modificacion: object
    hash: str


@dataclass
class ResultadoBronze:
    dataset: str
    archivos_detectados: int = 0
    archivos_procesados: int = 0
    archivos_omitidos: int = 0
    registros: int = 0
    columnas_nuevas: list[str] = field(default_factory=list)
    incumplimientos: list[str] = field(default_factory=list)
    watermark: str | None = None


def nombre_tabla_bronze(dataset: str) -> str:
    return f"bronze_{dataset}"


def listar_archivos(spark: SparkSession, cfg: PipelineConfig, dataset: str) -> list[ArchivoFuente]:
    """Lista los archivos ``<dataset>*.csv|parquet`` del volumen con su hash de contenido.

    Solo se recolecta metadata de archivos (pocas filas), nunca el contenido del dataset.
    """
    try:
        df = (
            spark.read.format("binaryFile")
            .option("pathGlobFilter", f"{dataset}*.{{csv,parquet}}")
            .load(cfg.ruta_raw)
            .select(
                "path",
                F.element_at(F.split("path", "/"), -1).alias("nombre"),
                F.col("length").alias("tamano"),
                F.col("modificationTime").alias("modificacion"),
                F.sha2(F.col("content"), 256).alias("hash"),
            )
        )
        filas = df.orderBy("modificacion", "path").collect()
    except Exception as exc:
        if "PATH_NOT_FOUND" in str(exc) or "does not exist" in str(exc):
            raise FileNotFoundError(
                f"No existe la ruta de aterrizaje {cfg.ruta_raw}. Cargue los archivos en el volumen "
                f"(ver docs/guia_databricks_github.md)."
            ) from exc
        raise
    return [ArchivoFuente(r.path, r.nombre, r.tamano, r.modificacion, r.hash) for r in filas]


def leer_archivo(spark: SparkSession, archivo: ArchivoFuente) -> DataFrame:
    """Lee un archivo conservando todos los valores como texto y capturando filas malformadas."""
    if archivo.nombre.lower().endswith(".parquet"):
        df = spark.read.parquet(archivo.path)
        df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
        return normalizar_columnas(df).withColumn(COLUMNA_CORRUPTA, F.lit(None).cast("string"))

    encabezado = spark.read.options(**OPCIONES_CSV).csv(archivo.path).columns
    esquema = T.StructType(
        [T.StructField(c, T.StringType()) for c in encabezado]
        + [T.StructField(COLUMNA_CORRUPTA, T.StringType())]
    )
    df = (
        spark.read.options(**OPCIONES_CSV)
        .option("columnNameOfCorruptRecord", COLUMNA_CORRUPTA)
        .option("enforceSchema", "true")
        .schema(esquema)
        .csv(archivo.path)
    )
    return normalizar_columnas(df)


def agregar_metadatos(df: DataFrame, archivo: ArchivoFuente, cfg: PipelineConfig, dataset: str) -> DataFrame:
    """Agrega metadatos de ingesta y control. ``_row_hash`` solo considera columnas de negocio."""
    columnas_negocio = [c for c in df.columns if not c.startswith("_")]
    row_hash = F.sha2(F.to_json(F.struct(*[F.col(c) for c in columnas_negocio])), 256)
    df = df.select(
        *[F.col(c) for c in df.columns],
        F.lit(dataset).alias("_dataset"),
        F.lit(archivo.path).alias("_source_file"),
        F.lit(archivo.nombre).alias("_file_name"),
        F.lit(archivo.hash).alias("_file_hash"),
        F.lit(archivo.modificacion).cast("timestamp").alias("_file_modification_ts"),
        F.current_timestamp().alias("_ingestion_ts"),
        F.lit(cfg.run_id).alias("_run_id"),
        F.lit(cfg.fecha_corte).cast("date").alias("_fecha_corte"),
        row_hash.alias("_row_hash"),
    )
    # Identificador estable y reproducible de cada fila física del archivo (incluye duplicados exactos).
    ocurrencia = F.row_number().over(Window.partitionBy("_file_hash", "_row_hash").orderBy("_row_hash"))
    return df.withColumn(
        "_record_id",
        F.sha2(F.concat_ws("|", F.col("_file_hash"), F.col("_row_hash"), ocurrencia.cast("string")), 256),
    )


def _hashes_registrados(spark: SparkSession, cfg: PipelineConfig, dataset: str) -> dict[str, str]:
    tabla = cfg.tabla(CTL_ARCHIVOS)
    if not tabla_existe(spark, tabla):
        return {}
    filas = spark.sql(f"SELECT file_hash, estado FROM {tabla} WHERE dataset = '{dataset}'").collect()
    return {r.file_hash: r.estado for r in filas}


def ingerir_dataset(spark: SparkSession, cfg: PipelineConfig, contrato: ContratoDataset) -> ResultadoBronze:
    """Ingiere los archivos nuevos (o todos, en reproceso) de un dataset a ``bronze_<dataset>``."""
    log = get_logger("bronze", cfg.run_id)
    dataset = contrato.nombre
    resultado = ResultadoBronze(dataset=dataset)
    archivos = listar_archivos(spark, cfg, dataset)
    resultado.archivos_detectados = len(archivos)
    if not archivos:
        log.warning(f"[{dataset}] No se encontraron archivos en {cfg.ruta_raw}")
        return resultado

    registrados = _hashes_registrados(spark, cfg, dataset)
    pendientes_revision = [a.nombre for a in archivos if registrados.get(a.hash) == "PENDIENTE_REVISION"]
    if pendientes_revision:
        resultado.incumplimientos.append(
            f"Archivos en PENDIENTE_REVISION por incumplir contrato: {pendientes_revision}"
        )

    vistos: set[str] = set()
    a_procesar: list[ArchivoFuente] = []
    for a in archivos:
        if a.hash in vistos:
            log.info(f"[{dataset}] {a.nombre}: mismo contenido que otro archivo del lote, se omite")
            continue
        vistos.add(a.hash)
        if cfg.modo == "incremental" and a.hash in registrados:
            log.info(f"[{dataset}] {a.nombre}: ya ingerido (hash {a.hash[:12]}), se omite")
            continue
        a_procesar.append(a)
    resultado.archivos_omitidos = len(archivos) - len(a_procesar)
    resultado.watermark = str(max(a.modificacion for a in archivos))

    if not a_procesar:
        log.info(f"[{dataset}] Sin archivos nuevos. Bronze no cambia (idempotente).")
        return resultado

    tabla = cfg.tabla(nombre_tabla_bronze(dataset))
    columnas_previas = set(spark.table(tabla).columns) if tabla_existe(spark, tabla) else set()
    lote: DataFrame | None = None
    filas_control = []
    for a in a_procesar:
        df = agregar_metadatos(leer_archivo(spark, a), a, cfg, dataset)
        columnas = [c for c in df.columns if not c.startswith("_")]
        validacion = validar_contrato(columnas, contrato)
        estado, motivo = "INGERIDO", None
        if not validacion.cumple:
            estado = "PENDIENTE_REVISION"
            motivo = f"Faltan columnas obligatorias: {validacion.faltantes_requeridas}"
            resultado.incumplimientos.append(f"{a.nombre}: {motivo}")
        nuevas = sorted(set(columnas) - columnas_previas) if columnas_previas else []
        resultado.columnas_nuevas.extend(c for c in nuevas if c not in resultado.columnas_nuevas)
        filas_control.append({
            "dataset": dataset, "file_hash": a.hash, "file_path": a.path, "file_name": a.nombre,
            "file_size": a.tamano, "file_modification_ts": a.modificacion,
            "columnas": ",".join(columnas),
            "schema_hash": hashlib.sha256(",".join(sorted(columnas)).encode()).hexdigest()[:16],
            "columnas_nuevas": ",".join(nuevas) or None,
            "columnas_faltantes": ",".join(validacion.faltantes_requeridas + validacion.faltantes_opcionales) or None,
            "estado": estado, "motivo_estado": motivo, "run_id": cfg.run_id,
            "fecha_corte": cfg.fecha_corte, "ts_ingesta": ahora_utc(),
        })
        lote = df if lote is None else lote.unionByName(df, allowMissingColumns=True)

    hashes = ", ".join(f"'{a.hash}'" for a in a_procesar)
    escribir_delta(
        lote, tabla,
        replace_where=f"_file_hash IN ({hashes})",
        merge_schema=True,
        propiedades={"delta.enableChangeDataFeed": "true", "capa": "bronze"},
        comentario=f"Bronze {dataset}: copia fiel de {contrato.sistema_origen} con metadatos de ingesta",
    )

    # Conteo por archivo (agregado pequeño) para control y reconciliación.
    conteos = {
        r["_file_hash"]: r["n"]
        for r in spark.table(tabla).where(f"_file_hash IN ({hashes})")
        .groupBy("_file_hash").agg(F.count(F.lit(1)).alias("n")).collect()
    }
    for f in filas_control:
        f["registros"] = conteos.get(f["file_hash"], 0)
    upsert_control(spark, cfg, CTL_ARCHIVOS, filas_control, ["dataset", "file_hash"])

    resultado.archivos_procesados = len(a_procesar)
    resultado.registros = int(sum(conteos.values()))
    log.info(
        f"[{dataset}] Bronze: {resultado.archivos_procesados} archivo(s), {resultado.registros} registros, "
        f"columnas nuevas={resultado.columnas_nuevas or '-'}"
    )
    return resultado
