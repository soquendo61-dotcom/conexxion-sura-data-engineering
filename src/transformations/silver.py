"""Construcción de la capa Silver (funciones puras: DataFrame -> DataFrame).

Flujo por dataset:
    Bronze (archivos válidos)
      -> aplicar contrato (alias/esquema) -> limpieza -> tipado (con registro de errores)
      -> duplicados exactos -> versión vigente por clave -> duplicados de negocio
      -> enriquecimiento (integridad referencial y atributos derivados)
      -> reglas de calidad -> aceptados (Silver) | cuarentena | versiones reemplazadas
      -> reconciliación Bronze vs (Silver + cuarentena + reemplazadas)

La persistencia se hace en ``pipeline.tasks`` para poder probar esta lógica sin Delta.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.config import PipelineConfig
from contracts.definitions import CONTRATOS, EVENTOS_POR_ENTIDAD, ContratoDataset
from quality import engine
from quality.rules import reglas_para
from transformations.dedup import (
    marcar_duplicados_exactos,
    marcar_duplicados_negocio,
    seleccionar_version_vigente,
)
from transformations.standardize import COLUMNA_ERRORES_TIPO, aplicar_contrato, limpiar_textos, tipar

LINAJE = ["_record_id", "_source_file", "_file_hash", "_row_hash", "_ingestion_ts", "_run_id"]
ESQUEMA_PAYLOAD = T.StructType([
    T.StructField("estado_origen", T.StringType()),
    T.StructField("estado_destino", T.StringType()),
    T.StructField("usuario_proceso", T.StringType()),
])
ESTADOS_SINIESTRO_ABIERTO = ("Abierto", "En investigacion", "Reactivado")


@dataclass
class SalidaSilver:
    dataset: str
    aceptados: DataFrame
    cuarentena: DataFrame
    evaluado: DataFrame
    reglas: list
    columnas_silver: list[str]
    metricas: dict = field(default_factory=dict)


def nombre_tabla_silver(dataset: str) -> str:
    return f"silver_{dataset}"


# --------------------------------------------------------------------------- preparación
def preparar(bronze: DataFrame, contrato: ContratoDataset) -> DataFrame:
    """Contrato + limpieza + tipado. Conserva el registro original para la cuarentena."""
    if "_corrupt_record" not in bronze.columns:
        bronze = bronze.withColumn("_corrupt_record", F.lit(None).cast("string"))
    df = aplicar_contrato(bronze, contrato)
    df = df.withColumn("_registro_original", F.to_json(F.struct(*[F.col(c) for c in contrato.nombres])))
    df = limpiar_textos(df, contrato.nombres)
    return tipar(df, contrato)


def clasificar_versiones(df: DataFrame, contrato: ContratoDataset) -> DataFrame:
    df = marcar_duplicados_exactos(df)
    df = seleccionar_version_vigente(df, contrato)
    return marcar_duplicados_negocio(df, contrato)


# --------------------------------------------------------------------------- enriquecimiento
def _claves(df: DataFrame, clave: str, alias: str) -> DataFrame:
    return df.select(F.col(clave).alias(alias)).distinct()


def enriquecer(df: DataFrame, dataset: str, padres: dict[str, DataFrame], cfg: PipelineConfig) -> DataFrame:
    """Atributos derivados e integridad referencial contra los padres ya aceptados en Silver."""
    if dataset == "clientes":
        w = Window.partitionBy("tipo_documento", "numero_documento", "_es_version_actual")
        compartido = (F.size(F.collect_set("cliente_id").over(w)) > 1) & F.col("numero_documento").isNotNull()
        return df.withColumn("_documento_compartido", compartido & F.col("_es_version_actual"))

    if dataset == "polizas":
        clientes = _claves(padres["clientes"], "cliente_id", "__cli")
        return (
            df.join(F.broadcast(clientes), F.col("cliente_id") == F.col("__cli"), "left")
            .withColumn("_fk_cliente_ok", F.col("__cli").isNotNull())
            .withColumn("dias_vigencia", F.datediff("fecha_fin_vigencia", "fecha_inicio_vigencia"))
            .drop("__cli")
        )

    if dataset in ("recaudos", "siniestros"):
        polizas = padres["polizas"].select(
            F.col("poliza_id").alias("__pol"),
            F.col("fecha_inicio_vigencia").alias("__ini"),
            F.col("fecha_fin_vigencia").alias("__fin"),
        )
        df = df.join(F.broadcast(polizas), F.col("poliza_id") == F.col("__pol"), "left")
        df = df.withColumn("_fk_poliza_ok", F.col("__pol").isNotNull())
        if dataset == "recaudos":
            df = (
                df.withColumn("_pago_en_vigencia", F.col("fecha_pago").between(F.col("__ini"), F.col("__fin")))
                .withColumn("dias_rezago_ingesta", F.datediff("fecha_ingesta", "fecha_pago"))
            )
        else:
            df = (
                df.withColumn("_ocurrencia_en_vigencia",
                              F.col("fecha_ocurrencia").between(F.col("__ini"), F.col("__fin")))
                .withColumn("dias_aviso", F.datediff("fecha_aviso", "fecha_ocurrencia"))
                .withColumn("costo_incurrido", (F.col("valor_pagado") + F.col("valor_reserva")).cast("decimal(18,2)"))
                .withColumn("siniestro_abierto", F.col("estado").isin(*ESTADOS_SINIESTRO_ABIERTO))
            )
        return df.drop("__pol", "__ini", "__fin")

    if dataset == "movimientos":
        # Mapa entidad -> póliza relacionada y existencia de la entidad referenciada.
        mapa = (
            padres["polizas"].select(F.lit("POLIZA").alias("__ent"), F.col("poliza_id").alias("__id"),
                                     F.col("poliza_id").alias("poliza_id_relacionada"))
            .unionByName(padres["siniestros"].select(F.lit("SINIESTRO").alias("__ent"),
                                                     F.col("siniestro_id").alias("__id"),
                                                     F.col("poliza_id").alias("poliza_id_relacionada")))
            .unionByName(padres["recaudos"].select(F.lit("RECAUDO").alias("__ent"),
                                                   F.col("recaudo_id").alias("__id"),
                                                   F.col("poliza_id").alias("poliza_id_relacionada")))
            .unionByName(padres["clientes"].select(F.lit("CLIENTE").alias("__ent"),
                                                   F.col("cliente_id").alias("__id"),
                                                   F.lit(None).cast("string").alias("poliza_id_relacionada")))
        )
        coherente = F.lit(False)
        for entidad, eventos in EVENTOS_POR_ENTIDAD.items():
            coherente = coherente | ((F.col("entidad") == entidad) & F.col("tipo_evento").isin(*eventos))
        payload = F.from_json(F.col("payload"), ESQUEMA_PAYLOAD)
        return (
            df.join(F.broadcast(mapa), (F.col("entidad") == F.col("__ent")) & (F.col("id_entidad") == F.col("__id")),
                    "left")
            .withColumn("_fk_entidad_ok", F.col("__id").isNotNull())
            .withColumn("_payload", payload)
            .withColumn("_payload_valido", F.col("payload").isNull() | F.col("_payload").isNotNull())
            .withColumn("estado_origen", F.col("_payload.estado_origen"))
            .withColumn("estado_destino", F.col("_payload.estado_destino"))
            .withColumn("usuario_proceso", F.col("_payload.usuario_proceso"))
            .withColumn("_evento_coherente", coherente)
            .withColumn("dias_rezago_ingesta", F.datediff("fecha_ingesta", "fecha_evento"))
            .withColumn("_llegada_tardia", F.coalesce(F.col("dias_rezago_ingesta") > cfg.dias_llegada_tardia,
                                                      F.lit(False)))
            .drop("__ent", "__id", "_payload")
        )
    raise ValueError(f"Dataset desconocido: {dataset}")


COLUMNAS_DERIVADAS = {
    "clientes": [],
    "polizas": ["dias_vigencia", "_fk_cliente_ok"],
    "recaudos": ["dias_rezago_ingesta", "_fk_poliza_ok", "_pago_en_vigencia"],
    "siniestros": ["costo_incurrido", "siniestro_abierto", "dias_aviso", "_fk_poliza_ok", "_ocurrencia_en_vigencia"],
    "movimientos": ["estado_origen", "estado_destino", "usuario_proceso", "poliza_id_relacionada",
                    "dias_rezago_ingesta", "_llegada_tardia", "_fk_entidad_ok"],
}


# --------------------------------------------------------------------------- construcción
def construir_silver(bronze: DataFrame, dataset: str, padres: dict[str, DataFrame],
                     cfg: PipelineConfig) -> SalidaSilver:
    contrato = CONTRATOS[dataset]
    reglas = reglas_para(dataset, cfg)

    preparado = clasificar_versiones(preparar(bronze, contrato), contrato)
    # Se evalúan las versiones vigentes y los duplicados exactos; las versiones reemplazadas
    # no se re-evalúan (son historia) y se contabilizan en la reconciliación.
    poblacion = preparado.filter(F.col("_es_version_actual") | F.col("_dup_exacto"))
    evaluado = engine.evaluar_reglas(enriquecer(poblacion, dataset, padres, cfg), reglas)

    columnas_silver = (
        contrato.nombres + COLUMNAS_DERIVADAS[dataset] + ["_dq_advertencias"] + LINAJE
    )
    aceptados = (
        engine.aceptados(evaluado, reglas)
        .select(*columnas_silver)
        .withColumnRenamed("_run_id", "_bronze_run_id")
        .withColumn("_silver_hash", F.sha2(F.to_json(F.struct(
            *[F.col(c) for c in contrato.nombres + COLUMNAS_DERIVADAS[dataset] + ["_dq_advertencias"]]
        )), 256))
        .withColumn("_silver_run_id", F.lit(cfg.run_id))
        .withColumn("_silver_ts", F.current_timestamp())
    )
    cuarentena = engine.construir_cuarentena(
        evaluado, reglas, dataset=dataset, clave=contrato.clave, run_id=cfg.run_id,
        pipeline=cfg.pipeline, fecha_corte=cfg.fecha_corte_str,
    )
    return SalidaSilver(dataset, aceptados, cuarentena, evaluado, reglas, columnas_silver)


def reconciliar_bronze_silver(bronze: DataFrame, salida: SalidaSilver, dataset: str) -> list[dict]:
    """Bronze = aceptados + rechazados (cuarentena) + versiones reemplazadas.

    Se reconcilian conteos, claves de negocio y totales monetarios. Los totales se calculan
    sobre los valores tipados de Bronze para comparar en la misma unidad.
    """
    contrato = CONTRATOS[dataset]
    clave = contrato.clave
    preparado = clasificar_versiones(preparar(bronze, contrato), contrato)
    destino = (
        preparado.join(
            salida.evaluado.select("_record_id", F.col("_en_cuarentena")), "_record_id", "left"
        )
        .withColumn(
            "_destino",
            F.when(F.col("_en_cuarentena").isNull(), "REEMPLAZADO")
            .when(F.col("_en_cuarentena"), "CUARENTENA").otherwise("SILVER"),
        )
    )
    montos = contrato.columnas_monetarias
    aggs = [
        F.count(F.lit(1)).alias("bronze_registros"),
        F.countDistinct(clave).alias("bronze_claves"),
        F.sum(F.when(F.col("_destino") == "SILVER", 1).otherwise(0)).alias("silver"),
        F.sum(F.when(F.col("_destino") == "CUARENTENA", 1).otherwise(0)).alias("cuarentena"),
        F.sum(F.when(F.col("_destino") == "REEMPLAZADO", 1).otherwise(0)).alias("reemplazados"),
        F.countDistinct(F.when(F.col("_destino") != "REEMPLAZADO", F.col(clave))).alias("claves_destino"),
    ]
    for m in montos:
        aggs += [
            F.sum(m).alias(f"{m}_bronze"),
            F.sum(F.when(F.col("_destino") == "SILVER", F.col(m))).alias(f"{m}_silver"),
            F.sum(F.when(F.col("_destino") == "CUARENTENA", F.col(m))).alias(f"{m}_cuarentena"),
            F.sum(F.when(F.col("_destino") == "REEMPLAZADO", F.col(m))).alias(f"{m}_reemplazado"),
        ]
    r = destino.agg(*aggs).collect()[0].asDict()

    def fila(regla_id: str, descripcion: str, esperado, obtenido, ok: bool) -> dict:
        return {
            "dataset": dataset, "tipo_control": "RECONCILIACION", "regla_id": regla_id,
            "dimension": "consistencia", "descripcion": descripcion, "severidad": "BLOQUEO",
            "accion": None, "registros_evaluados": int(r["bronze_registros"]),
            "registros_fallidos": 0 if ok else 1, "pct_fallo": 0.0 if ok else 100.0, "umbral_pct": 0.0,
            "valor_esperado": str(esperado), "valor_obtenido": str(obtenido),
            "resultado": "OK" if ok else "FALLA",
        }

    obtenido = r["silver"] + r["cuarentena"] + r["reemplazados"]
    filas = [
        fila("RECON_BRONZE_SILVER_CONTEO", "Registros Bronze = Silver + cuarentena + versiones reemplazadas",
             r["bronze_registros"], f"{obtenido} (silver={r['silver']}, cuarentena={r['cuarentena']}, "
             f"reemplazados={r['reemplazados']})", obtenido == r["bronze_registros"]),
        fila("RECON_BRONZE_SILVER_CLAVES", "Claves de negocio Bronze = claves en Silver + cuarentena",
             r["bronze_claves"], r["claves_destino"], r["bronze_claves"] == r["claves_destino"]),
    ]
    for m in montos:
        total = sum((r[f"{m}_{d}"] or 0) for d in ("silver", "cuarentena", "reemplazado"))
        filas.append(fila(
            f"RECON_BRONZE_SILVER_{m.upper()}", f"Total monetario {m}: Bronze = Silver + cuarentena + reemplazados",
            r[f"{m}_bronze"], f"{total} (silver={r[f'{m}_silver']}, cuarentena={r[f'{m}_cuarentena']})",
            (r[f"{m}_bronze"] or 0) == total,
        ))
    salida.metricas = {
        "registros_entrada": int(r["bronze_registros"]), "registros_aceptados": int(r["silver"]),
        "registros_rechazados": int(r["cuarentena"]), "versiones_reemplazadas": int(r["reemplazados"]),
    }
    return filas


__all__ = ["construir_silver", "reconciliar_bronze_silver", "nombre_tabla_silver", "preparar",
           "COLUMNA_ERRORES_TIPO"]
