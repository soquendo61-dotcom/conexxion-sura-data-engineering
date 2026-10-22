"""Historización SCD tipo 2 (dimensión de clientes).

Decisión: Silver mantiene el estado vigente de cada entidad (SCD1, consumo simple) y para la
dimensión de clientes se publica además ``silver_clientes_historia`` (SCD2) con vigencias
``valido_desde``/``valido_hasta``. Se reconstruye completa desde Bronze en cada ejecución:
es determinística e idempotente, y para el volumen de clientes es más simple y segura que un
MERGE incremental. A gran escala se cambiaría a MERGE con Change Data Feed (ver docs).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

FECHA_ABIERTA = "9999-12-31"


def construir_scd2(versiones: DataFrame, clave: str, columna_version: str, atributos: list[str]) -> DataFrame:
    """Una fila por (clave, versión distinta de atributos) con su periodo de validez.

    ``versiones`` debe contener todas las versiones válidas (sin duplicados exactos).
    Versiones consecutivas con atributos idénticos se colapsan (no generan historia falsa).
    """
    w_dia = Window.partitionBy(clave, columna_version).orderBy(F.col("_ingestion_ts").desc(), F.col("_row_hash").desc())
    hash_attr = F.sha2(F.to_json(F.struct(*[F.col(c) for c in atributos])), 256)
    df = (
        versiones.filter(F.col(clave).isNotNull() & F.col(columna_version).isNotNull())
        .withColumn("__rn", F.row_number().over(w_dia))
        .filter("__rn = 1")
        .withColumn("_hash_atributos", hash_attr)
    )
    w = Window.partitionBy(clave).orderBy(columna_version)
    df = (
        df.withColumn("__prev", F.lag("_hash_atributos").over(w))
        .filter(F.col("__prev").isNull() | (F.col("__prev") != F.col("_hash_atributos")))
    )
    siguiente = F.lead(columna_version).over(w)
    return (
        df.select(
            clave, *[c for c in atributos if c != clave], "_hash_atributos", "_record_id", "_source_file",
            F.col(columna_version).alias("valido_desde"),
            F.coalesce(F.date_sub(siguiente, 1), F.lit(FECHA_ABIERTA).cast("date")).alias("valido_hasta"),
            siguiente.isNull().alias("es_actual"),
        )
        .withColumn("version", F.row_number().over(Window.partitionBy(clave).orderBy("valido_desde")))
    )
