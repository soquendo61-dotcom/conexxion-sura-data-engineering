"""Validación local de la lógica Silver/Gold con los CSV reales, sin Delta (solo en memoria).

Uso:
    python scripts/validacion_local.py --ruta "<carpeta con los CSV>" --fecha-corte 2026-08-31

Simula Bronze en memoria (mismas funciones de lectura y metadatos que en Databricks), ejecuta
Silver y Gold y muestra resúmenes. No escribe nada en disco.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import PipelineConfig  # noqa: E402
from contracts.definitions import ORDEN_PROCESAMIENTO  # noqa: E402
from ingestion.bronze import ArchivoFuente, agregar_metadatos, leer_archivo  # noqa: E402
from quality import engine  # noqa: E402

from transformations.silver import construir_silver, reconciliar_bronze_silver  # noqa: E402


def spark_local() -> SparkSession:
    if os.name == "nt":
        os.environ.setdefault("JAVA_TOOL_OPTIONS", "-Djdk.net.unixdomain.tmpdir=C:/Users/Public/sparktmp")
    return (
        SparkSession.builder.master("local[2]").appName("validacion_local")
        .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "America/Bogota")
        .getOrCreate()
    )


def bronze_en_memoria(spark, cfg, ruta: Path, dataset: str):
    df = None
    for f in sorted(ruta.glob(f"{dataset}*.csv")):
        a = ArchivoFuente(str(f), f.name, f.stat().st_size, datetime.fromtimestamp(f.stat().st_mtime),
                          hashlib.sha256(f.read_bytes()).hexdigest())
        d = agregar_metadatos(leer_archivo(spark, a), a, cfg, dataset)
        df = d if df is None else df.unionByName(d, allowMissingColumns=True)
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ruta", required=True)
    ap.add_argument("--fecha-corte", default="2026-08-31")
    ap.add_argument("--solo-silver", action="store_true")
    args = ap.parse_args()
    spark = spark_local()
    spark.sparkContext.setLogLevel("ERROR")
    cfg = PipelineConfig.desde_parametros({"fecha_corte": args.fecha_corte, "run_id": "local"})
    ruta = Path(args.ruta)

    silver = {}
    resultados = []
    for ds in ORDEN_PROCESAMIENTO:
        bronze = bronze_en_memoria(spark, cfg, ruta, ds)
        salida = construir_silver(bronze, ds, silver, cfg)
        silver[ds] = salida.aceptados
        res = engine.resumir(salida.evaluado, salida.reglas)
        rec = reconciliar_bronze_silver(bronze, salida, ds)
        resultados += res + rec
        print(f"\n=== {ds}: {salida.metricas}")
        for r in res + rec:
            if r["registros_fallidos"] or r["resultado"] != "OK":
                print(f"  {r['resultado']:<11} {r['regla_id']:<34} fallidos={r['registros_fallidos']:<5} "
                      f"pct={r['pct_fallo']:<8} {r['valor_obtenido'] if r['tipo_control']=='RECONCILIACION' else ''}")
        print("  cuarentena por regla:", {x["regla_id"]: x["n"] for x in
              salida.cuarentena.groupBy("regla_id").agg(F.count("*").alias("n")).collect()})
    print("\nBLOQUEOS:", [b["regla_id"] for b in engine.bloqueos(resultados)])
    if args.solo_silver:
        return

    from transformations import gold

    p360 = gold.construir_poliza_360(silver, cfg)
    sm = gold.construir_siniestralidad_mensual(silver, cfg)
    disc = gold.construir_discrepancias(silver, p360, cfg)
    print("\n=== gold_poliza_360:", p360.count(), "filas")
    p360.groupBy("estado_corte").count().orderBy("estado_corte").show()
    p360.select("poliza_id", "estado_fuente", "estado_corte", "prima_emitida", "recaudo_aplicado",
                "saldo_por_recaudar", "num_siniestros", "costo_incurrido", "ultima_actividad_fecha",
                "ultima_actividad_origen").show(5, truncate=False)
    print("=== gold_siniestralidad_mensual:", sm.count(), "filas")
    sm.orderBy(F.col("periodo").desc(), "producto", "sucursal").show(8, truncate=False)
    print("=== gold_discrepancias por tipo")
    disc.groupBy("tipo_discrepancia", "severidad").count().orderBy("tipo_discrepancia").show(truncate=False)
    for r in gold.reconciliar_silver_gold(silver, p360, sm, cfg) + gold.postcondiciones(p360, sm, cfg):
        print(f"  {r['resultado']:<6} {r['regla_id']:<40} esperado={r['valor_esperado']} obtenido={r['valor_obtenido']}")


if __name__ == "__main__":
    main()
