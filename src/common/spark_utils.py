"""Utilidades de Spark/Delta: sesión, escritura idempotente y MERGE."""

from __future__ import annotations

from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession

from common.logging_utils import get_logger

log = get_logger("spark_utils")


def get_spark() -> SparkSession:
    """Devuelve la sesión activa (Databricks) o crea una local (pruebas)."""
    activa = SparkSession.getActiveSession()
    return activa if activa is not None else SparkSession.builder.getOrCreate()


def tabla_existe(spark: SparkSession, nombre: str) -> bool:
    return spark.catalog.tableExists(nombre)


def _post_creacion(spark: SparkSession, nombre: str, cluster_by: Sequence[str] | None,
                   propiedades: dict[str, str] | None, comentario: str | None) -> None:
    """Ajustes que se aplican una sola vez cuando la tabla se crea."""
    sentencias = []
    if cluster_by:
        sentencias.append(f"ALTER TABLE {nombre} CLUSTER BY ({', '.join(cluster_by)})")
    if propiedades:
        props = ", ".join(f"'{k}' = '{v}'" for k, v in propiedades.items())
        sentencias.append(f"ALTER TABLE {nombre} SET TBLPROPERTIES ({props})")
    if comentario:
        sentencias.append(f"COMMENT ON TABLE {nombre} IS '{comentario.replace(chr(39), '')}'")
    for sql in sentencias:
        try:
            spark.sql(sql)
        except Exception as exc:  # característica no soportada en el entorno: no es crítico
            log.warning(f"No se pudo aplicar '{sql}' sobre {nombre}: {exc}")


def escribir_delta(
    df: DataFrame,
    nombre: str,
    *,
    replace_where: str | None = None,
    modo: str = "overwrite",
    merge_schema: bool = False,
    cluster_by: Sequence[str] | None = None,
    propiedades: dict[str, str] | None = None,
    comentario: str | None = None,
) -> None:
    """Escribe un DataFrame en una tabla Delta administrada de forma idempotente.

    - Si la tabla no existe la crea y aplica clustering/propiedades/comentario.
    - Con ``replace_where`` reemplaza atómicamente solo el subconjunto indicado (p. ej. un
      ``fecha_corte`` o los ``_file_hash`` reprocesados): reejecutar no duplica.
    """
    spark = df.sparkSession
    existe = tabla_existe(spark, nombre)
    writer = df.write.format("delta")
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if not existe:
        writer.mode("overwrite").saveAsTable(nombre)
        _post_creacion(spark, nombre, cluster_by, propiedades, comentario)
        return
    if replace_where:
        writer.mode("overwrite").option("replaceWhere", replace_where).saveAsTable(nombre)
    else:
        writer.mode(modo).saveAsTable(nombre)


def merge_por_clave(
    df: DataFrame,
    nombre: str,
    claves: Iterable[str],
    *,
    condicion_actualizacion: str | None = None,
    borrar_no_presentes: bool = False,
    condicion_no_presentes: str | None = None,
    actualizar_no_presentes: dict[str, str] | None = None,
    cluster_by: Sequence[str] | None = None,
    propiedades: dict[str, str] | None = None,
    comentario: str | None = None,
) -> None:
    """MERGE (upsert) por clave de negocio.

    - Inserta claves nuevas, actualiza solo si ``condicion_actualizacion`` es verdadera
      (p. ej. cambió el hash del registro), evitando reescrituras innecesarias.
    - Opcionalmente borra o marca claves que ya no vienen en la fuente (sincronización).
    """
    from delta.tables import DeltaTable  # disponible en Databricks / delta-spark

    spark = df.sparkSession
    if not tabla_existe(spark, nombre):
        escribir_delta(df, nombre, cluster_by=cluster_by, propiedades=propiedades, comentario=comentario)
        return
    on = " AND ".join(f"t.{c} <=> s.{c}" for c in claves)
    merge = DeltaTable.forName(spark, nombre).alias("t").merge(df.alias("s"), on)
    merge = merge.whenMatchedUpdateAll(condition=condicion_actualizacion).whenNotMatchedInsertAll()
    if borrar_no_presentes:
        merge = merge.whenNotMatchedBySourceDelete(condition=condicion_no_presentes)
    elif actualizar_no_presentes:
        merge = merge.whenNotMatchedBySourceUpdate(condition=condicion_no_presentes,
                                                   set=actualizar_no_presentes)
    merge.execute()
