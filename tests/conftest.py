"""Fixtures compartidas: sesión Spark (local o Databricks) y constructor de lotes Bronze sintéticos."""

from __future__ import annotations

import hashlib
import os
import sys
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common.config import PipelineConfig  # noqa: E402
from ingestion.bronze import ArchivoFuente, agregar_metadatos  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        yield SparkSession.builder.getOrCreate()
        return
    # Los workers de Python deben usar el mismo intérprete del entorno de pruebas.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    if os.name == "nt":  # Java en Windows necesita un directorio temporal sin espacios para sockets locales
        os.environ.setdefault("JAVA_TOOL_OPTIONS", "-Djdk.net.unixdomain.tmpdir=C:/Users/Public/sparktmp")
    sesion = (
        SparkSession.builder.master("local[1]").appName("pruebas")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "America/Bogota")
        .getOrCreate()
    )
    sesion.sparkContext.setLogLevel("ERROR")
    yield sesion
    sesion.stop()


@pytest.fixture(scope="session")
def cfg():
    return PipelineConfig.desde_parametros({"fecha_corte": "2026-08-31", "run_id": "test"})


@pytest.fixture(scope="session")
def bronze(spark, cfg):
    """Construye un lote Bronze con los mismos metadatos que la ingesta real.

    ``filas`` es una lista de diccionarios (todos los valores como texto, igual que en Bronze).
    """

    def _construir(dataset: str, filas: list[dict], archivo: str = "archivo_1.csv",
                   modificacion: datetime = datetime(2026, 9, 1)):
        columnas = list(dict.fromkeys(c for f in filas for c in f))
        datos = [tuple(f.get(c) for c in columnas) for f in filas]
        df = spark.createDataFrame(datos, ", ".join(f"{c} string" for c in columnas))
        df = df.withColumn("_corrupt_record", F.lit(None).cast("string"))
        a = ArchivoFuente(f"/tmp/{archivo}", archivo, 1, modificacion, hashlib.sha256(archivo.encode()).hexdigest())
        return agregar_metadatos(df, a, cfg, dataset)

    return _construir
