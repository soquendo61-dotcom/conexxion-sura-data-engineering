"""Motor de reglas de calidad declarativas.

Cada regla es una expresión SQL booleana que debe ser VERDADERA para que el registro pase
(un resultado null se considera incumplimiento). Además de la severidad, cada regla tiene una
acción a nivel de registro y un umbral a nivel de dataset:

- acción CUARENTENA: el registro se separa a ``silver_cuarentena`` con la regla incumplida.
- acción MARCAR: el registro continúa a Silver y la regla queda en ``_dq_advertencias``.
- umbral_pct: % máximo de incumplimiento tolerado. Si se supera:
    BLOQUEO     -> resultado FALLA: la ejecución falla después de registrar evidencias y
                   Gold NO se publica.
    ADVERTENCIA -> resultado ADVERTENCIA: se alerta y el pipeline continúa.
    INFORMATIVA -> resultado INFO: solo métrica.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

SEVERIDADES = ("BLOQUEO", "ADVERTENCIA", "INFORMATIVA")
ACCIONES = ("CUARENTENA", "MARCAR")
DIMENSIONES = ("completitud", "unicidad", "validez", "consistencia", "integridad_referencial", "oportunidad")


class CalidadBloqueanteError(RuntimeError):
    """Se superó el umbral de una regla de severidad BLOQUEO."""


@dataclass(frozen=True)
class Regla:
    id: str
    dataset: str
    dimension: str
    descripcion: str
    expresion: str
    severidad: str
    accion: str
    umbral_pct: float = 0.0

    def __post_init__(self) -> None:
        if self.severidad not in SEVERIDADES:
            raise ValueError(f"{self.id}: severidad inválida {self.severidad}")
        if self.accion not in ACCIONES:
            raise ValueError(f"{self.id}: acción inválida {self.accion}")
        if self.dimension not in DIMENSIONES:
            raise ValueError(f"{self.id}: dimensión inválida {self.dimension}")

    @property
    def columna(self) -> str:
        return f"_ok_{self.id.lower()}"


def _falla(regla: Regla) -> Column:
    return ~F.col(regla.columna)


def evaluar_reglas(df: DataFrame, reglas: list[Regla]) -> DataFrame:
    """Agrega una columna booleana por regla, ``_en_cuarentena`` y ``_dq_advertencias``."""
    flags = [F.coalesce(F.expr(r.expresion), F.lit(False)).alias(r.columna) for r in reglas]
    df = df.select("*", *flags)
    de_cuarentena = [r for r in reglas if r.accion == "CUARENTENA"]
    de_marca = [r for r in reglas if r.accion == "MARCAR"]
    en_cuarentena = reduce(lambda a, b: a | b, [_falla(r) for r in de_cuarentena], F.lit(False))
    advertencias = (
        F.array_compact(F.array(*[F.when(_falla(r), F.lit(r.id)) for r in de_marca]))
        if de_marca else F.array().cast("array<string>")
    )
    return df.withColumn("_en_cuarentena", en_cuarentena).withColumn("_dq_advertencias", advertencias)


def columnas_tecnicas_reglas(reglas: list[Regla]) -> list[str]:
    return [r.columna for r in reglas] + ["_en_cuarentena"]


def aceptados(df_evaluado: DataFrame, reglas: list[Regla]) -> DataFrame:
    return df_evaluado.filter(~F.col("_en_cuarentena")).drop(*columnas_tecnicas_reglas(reglas))


def construir_cuarentena(
    df_evaluado: DataFrame,
    reglas: list[Regla],
    *,
    dataset: str,
    clave: str,
    run_id: str,
    pipeline: str,
    fecha_corte: str,
) -> DataFrame:
    """Una fila por (registro, regla de cuarentena incumplida) con causa y trazabilidad."""
    de_cuarentena = [r for r in reglas if r.accion == "CUARENTENA"]
    fallas = F.array_compact(F.array(*[
        F.when(_falla(r), F.struct(
            F.lit(r.id).alias("regla_id"),
            F.lit(r.dimension).alias("dimension"),
            F.lit(r.severidad).alias("severidad"),
            F.lit(r.descripcion).alias("descripcion_regla"),
        ))
        for r in de_cuarentena
    ]))
    return (
        df_evaluado.filter(F.col("_en_cuarentena"))
        .select(
            F.lit(dataset).alias("dataset"),
            F.col(clave).cast("string").alias("clave_negocio"),
            "_record_id",
            F.explode(fallas).alias("falla"),
            F.col("_registro_original").alias("registro_original"),
            "_source_file",
            "_file_hash",
            "_row_hash",
        )
        .select(
            "dataset", "clave_negocio", "_record_id", "falla.*", "registro_original",
            "_source_file", "_file_hash", "_row_hash",
            F.lit(run_id).alias("run_id"),
            F.lit(pipeline).alias("pipeline"),
            F.lit(fecha_corte).cast("date").alias("fecha_corte"),
            F.current_timestamp().alias("ts_cuarentena"),
            F.lit("PENDIENTE").alias("estado_tratamiento"),
        )
    )


def clasificar_resultado(regla: Regla, registros_fallidos: int, pct_fallo: float) -> str:
    if regla.severidad == "INFORMATIVA":
        return "INFO" if registros_fallidos else "OK"
    if pct_fallo <= regla.umbral_pct:
        return "OK"
    return "FALLA" if regla.severidad == "BLOQUEO" else "ADVERTENCIA"


def resumir(df_evaluado: DataFrame, reglas: list[Regla]) -> list[dict]:
    """Resultado por regla (una sola agregación; solo se recolecta una fila)."""
    aggs = [F.count(F.lit(1)).alias("__total")] + [
        F.sum(F.when(_falla(r), 1).otherwise(0)).alias(f"__{i}") for i, r in enumerate(reglas)
    ]
    fila = df_evaluado.agg(*aggs).collect()[0]
    total = int(fila["__total"] or 0)
    resultados = []
    for i, r in enumerate(reglas):
        fallidos = int(fila[f"__{i}"] or 0)
        pct = round(100.0 * fallidos / total, 4) if total else 0.0
        resultados.append({
            "dataset": r.dataset, "tipo_control": "REGLA", "regla_id": r.id, "dimension": r.dimension,
            "descripcion": r.descripcion, "severidad": r.severidad, "accion": r.accion,
            "registros_evaluados": total, "registros_fallidos": fallidos, "pct_fallo": pct,
            "umbral_pct": float(r.umbral_pct), "valor_esperado": f"<= {r.umbral_pct}%",
            "valor_obtenido": f"{pct}%", "resultado": clasificar_resultado(r, fallidos, pct),
        })
    return resultados


def bloqueos(resultados: list[dict]) -> list[dict]:
    return [r for r in resultados if r["resultado"] == "FALLA"]
