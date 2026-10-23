"""Estandarización: nombres de columnas, validación de contrato, limpieza y tipado."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.dates import parsear_fecha
from contracts.definitions import ContratoDataset

TOKENS_NULOS = ("", "null", "none", "nan", "n/a", "na", "nulo", "sin dato")
COLUMNA_ERRORES_TIPO = "_errores_tipo"


def normalizar_nombre_columna(nombre: str) -> str:
    """'﻿Fecha Pago ' -> 'fecha_pago' (sin BOM, tildes, espacios ni símbolos)."""
    limpio = nombre.replace("﻿", "").strip().lower()
    limpio = unicodedata.normalize("NFKD", limpio).encode("ascii", "ignore").decode("ascii")
    limpio = re.sub(r"[^a-z0-9]+", "_", limpio).strip("_")
    return limpio or "columna_sin_nombre"


def normalizar_columnas(df: DataFrame) -> DataFrame:
    """Normaliza nombres de negocio; las columnas técnicas (prefijo '_') se conservan."""
    return df.toDF(*[c if c.startswith("_") else normalizar_nombre_columna(c) for c in df.columns])


@dataclass
class ResultadoContrato:
    dataset: str
    faltantes_requeridas: list[str] = field(default_factory=list)
    faltantes_opcionales: list[str] = field(default_factory=list)
    adicionales: list[str] = field(default_factory=list)
    renombradas: dict[str, str] = field(default_factory=dict)

    @property
    def cumple(self) -> bool:
        return not self.faltantes_requeridas


def validar_contrato(columnas: list[str], contrato: ContratoDataset) -> ResultadoContrato:
    """Compara las columnas recibidas (ya normalizadas) contra el contrato del dataset."""
    alias = contrato.mapa_alias
    renombradas = {c: alias[c] for c in columnas if c in alias and alias[c] not in columnas}
    efectivas = {renombradas.get(c, c) for c in columnas if not c.startswith("_")}
    return ResultadoContrato(
        dataset=contrato.nombre,
        faltantes_requeridas=[c for c in contrato.requeridas if c not in efectivas],
        faltantes_opcionales=[c for c in contrato.nombres if c not in efectivas and c not in contrato.requeridas],
        adicionales=sorted(c for c in efectivas if c not in contrato.nombres),
        renombradas=renombradas,
    )


def aplicar_contrato(df: DataFrame, contrato: ContratoDataset) -> DataFrame:
    """Renombra alias al nombre canónico y agrega como null las columnas opcionales ausentes.

    Las columnas adicionales no contratadas NO se propagan a Silver (quedan en Bronze) para
    proteger a los consumidores de cambios de esquema no acordados.
    """
    for origen, destino in contrato.mapa_alias.items():
        if origen not in df.columns:
            continue
        if destino in df.columns:
            # Archivos con esquema viejo y nuevo en el mismo lote: cada fila trae uno de los dos.
            df = df.withColumn(destino, F.coalesce(F.col(destino), F.col(origen))).drop(origen)
        else:
            df = df.withColumnRenamed(origen, destino)
    for c in contrato.nombres:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    metadatos = [c for c in df.columns if c.startswith("_")]
    return df.select(*contrato.nombres, *metadatos)


def limpiar_textos(df: DataFrame, columnas: list[str]) -> DataFrame:
    """Trim y conversión de tokens de vacío ('', 'NULL', 'N/A'...) a null real."""
    exprs = []
    for c in df.columns:
        if c in columnas:
            v = F.trim(F.col(c))
            exprs.append(F.when(F.lower(v).isin(*TOKENS_NULOS), None).otherwise(v).alias(c))
        else:
            exprs.append(F.col(c))
    return df.select(*exprs)


def tipar(df: DataFrame, contrato: ContratoDataset) -> DataFrame:
    """Convierte cada columna al tipo del contrato sin perder información.

    Si un valor no nulo no puede convertirse, el resultado queda null y el nombre de la
    columna se registra en ``_errores_tipo`` para que la regla de validez lo envíe a cuarentena.
    """
    errores = []
    exprs = {}
    for col in contrato.columnas:
        crudo = F.col(col.nombre)
        if col.tipo == "date":
            tipado = parsear_fecha(crudo)
        elif col.tipo == "decimal":
            tipado = crudo.try_cast("decimal(18,2)")
        elif col.tipo == "int":
            tipado = crudo.try_cast("int")
        else:
            continue
        exprs[col.nombre] = tipado
        errores.append(F.when(crudo.isNotNull() & tipado.isNull(), F.lit(col.nombre)))
    df = df.withColumn(
        COLUMNA_ERRORES_TIPO,
        F.array_compact(F.array(*errores)) if errores else F.array().cast("array<string>"),
    )
    return df.withColumns(exprs) if exprs else df
