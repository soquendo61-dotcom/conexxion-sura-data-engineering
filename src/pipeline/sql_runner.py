"""Ejecución de scripts SQL versionados (resources/sql) con variables ``${nombre}``."""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import SparkSession

from common.logging_utils import get_logger


def leer_sentencias(ruta: str | Path, variables: dict[str, str]) -> list[str]:
    texto = Path(ruta).read_text(encoding="utf-8")
    texto = "\n".join(linea for linea in texto.splitlines() if not linea.strip().startswith("--"))
    for clave, valor in variables.items():
        texto = texto.replace("${" + clave + "}", valor)
    pendientes = sorted(set(re.findall(r"\$\{(\w+)\}", texto)))
    if pendientes:
        raise ValueError(f"Variables sin valor en {ruta}: {pendientes}")
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", texto) if s.strip()]


def ejecutar_sql(spark: SparkSession, ruta: str | Path, variables: dict[str, str],
                 tolerar_errores: bool = False) -> list[dict]:
    """Ejecuta cada sentencia. Con ``tolerar_errores`` registra y continúa (útil para
    capacidades que pueden no existir en Free Edition, p. ej. grupos o máscaras)."""
    log = get_logger("sql_runner")
    resultado = []
    for sql in leer_sentencias(ruta, variables):
        resumen = " ".join(sql.split())[:140]
        try:
            spark.sql(sql)
            resultado.append({"sentencia": resumen, "estado": "OK", "detalle": None})
        except Exception as exc:
            if not tolerar_errores:
                raise
            detalle = str(exc).splitlines()[0][:300]
            log.warning(f"No aplicada: {resumen} -> {detalle}")
            resultado.append({"sentencia": resumen, "estado": "NO_APLICADA", "detalle": detalle})
    return resultado
