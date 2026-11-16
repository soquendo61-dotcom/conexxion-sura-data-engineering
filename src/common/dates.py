"""Utilidades de fechas.

Las fuentes traen fechas en tres formatos: ``yyyy-MM-dd``, ``yyyy/MM/dd`` y ``dd/MM/yyyy``.
Supuesto documentado: el formato con barras y año al final es día/mes/año (convención
colombiana), nunca mes/día/año.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

FORMATOS_FECHA: tuple[str, ...] = ("yyyy-MM-dd", "yyyy/MM/dd", "dd/MM/yyyy")


def parsear_fecha(col: Column | str, formatos: tuple[str, ...] = FORMATOS_FECHA) -> Column:
    """Convierte texto a fecha probando cada formato; devuelve null si ninguno aplica.

    Usa ``try_to_timestamp`` para que un valor inválido no rompa el job (modo ANSI) y quede
    disponible para la regla de calidad de validez de tipos.
    """
    c = F.trim(F.col(col) if isinstance(col, str) else col)
    intentos = [F.to_date(F.try_to_timestamp(c, F.lit(fmt))) for fmt in formatos]
    return F.coalesce(*intentos)
