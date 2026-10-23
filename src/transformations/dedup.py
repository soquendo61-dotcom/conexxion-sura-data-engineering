"""Deduplicación y selección de versiones.

Se distinguen tres situaciones (todas trazables, ninguna se elimina en silencio):

1. Duplicado exacto: la misma fila repetida dentro del mismo archivo -> cuarentena.
   (La misma fila recibida en OTRO archivo es un reenvío legítimo de una nueva entrega y se
   resuelve como versión; así una reejecución o una reentrega no duplica.)
2. Versiones de una misma clave: se conserva la más reciente según la columna de versión
   (``fecha_actualizacion``/``fecha_ingesta``) con desempate determinístico. Las anteriores
   quedan como versiones reemplazadas (historia en Bronze y SCD2).
3. Duplicado de negocio: el mismo hecho (huella de contenido idéntica) llega con otro
   identificador técnico -> se conserva el ID más antiguo y el reenvío va a cuarentena.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from contracts.definitions import ContratoDataset


def marcar_duplicados_exactos(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("_file_hash", "_row_hash").orderBy("_record_id")
    return df.withColumn("_dup_exacto", F.row_number().over(w) > 1)


def orden_version(contrato: ContratoDataset) -> list:
    """Criterio de la versión vigente: más reciente primero y desempate determinístico."""
    orden = []
    if contrato.columna_version:
        orden.append(F.col(contrato.columna_version).desc_nulls_last())
    orden += [F.col(c).desc_nulls_last() for c in contrato.columnas_orden_desempate]
    orden += [
        F.col("_file_modification_ts").desc_nulls_last(),
        F.col("_ingestion_ts").desc_nulls_last(),
        F.col("_row_hash").desc(),
    ]
    return orden


def seleccionar_version_vigente(df: DataFrame, contrato: ContratoDataset) -> DataFrame:
    """Agrega ``_es_version_actual`` y ``_empate_version`` (misma marca temporal, contenido distinto)."""
    clave = contrato.clave
    w = Window.partitionBy(clave, "_dup_exacto").orderBy(*orden_version(contrato))
    df = df.withColumn("_es_version_actual", (F.row_number().over(w) == 1) & ~F.col("_dup_exacto"))
    if contrato.columna_version:
        w_emp = Window.partitionBy(clave, "_dup_exacto", contrato.columna_version)
        empate = F.size(F.collect_set("_row_hash").over(w_emp)) > 1
        df = df.withColumn("_empate_version", empate & ~F.col("_dup_exacto"))
    else:
        df = df.withColumn("_empate_version", F.lit(False))
    return df


def marcar_duplicados_negocio(df: DataFrame, contrato: ContratoDataset) -> DataFrame:
    """Marca registros vigentes con huella de negocio idéntica a otro ID (conserva el ID menor)."""
    huella = F.sha2(F.to_json(F.struct(*[F.col(c) for c in contrato.columnas_huella])), 256)
    df = df.withColumn("_huella_negocio", huella)
    w = Window.partitionBy("_huella_negocio", "_es_version_actual").orderBy(F.col(contrato.clave).asc_nulls_last())
    return (
        df.withColumn("_id_original", F.first(contrato.clave).over(w))
        .withColumn(
            "_dup_negocio",
            F.col("_es_version_actual") & (F.col("_id_original") != F.col(contrato.clave)),
        )
    )
