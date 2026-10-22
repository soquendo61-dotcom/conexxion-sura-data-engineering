from datetime import date

import pytest
from pyspark.sql import functions as F

from common.config import ConfiguracionInvalidaError, PipelineConfig, ultimo_dia_mes_anterior
from common.dates import parsear_fecha
from pipeline.sql_runner import leer_sentencias


def test_fecha_corte_por_defecto_es_cierre_del_mes_anterior():
    assert ultimo_dia_mes_anterior(date(2026, 9, 5)) == date(2026, 8, 31)
    assert ultimo_dia_mes_anterior(date(2026, 3, 5)) == date(2026, 2, 28)
    assert PipelineConfig.desde_parametros({"fecha_corte": ""}).fecha_corte == ultimo_dia_mes_anterior(date.today())


def test_parametros_invalidos_fallan_con_mensaje_claro():
    with pytest.raises(ConfiguracionInvalidaError, match="modo"):
        PipelineConfig.desde_parametros({"modo": "borrar_todo"})
    with pytest.raises(ConfiguracionInvalidaError, match="YYYY-MM-DD"):
        PipelineConfig.desde_parametros({"fecha_corte": "31/08/2026"})


def test_rutas_y_nombres_parametrizados():
    cfg = PipelineConfig.desde_parametros({"catalog": "qa_seguros", "schema": "polizas", "volume": "landing"})
    assert cfg.tabla("silver_polizas") == "qa_seguros.polizas.silver_polizas"
    assert cfg.ruta_raw == "/Volumes/qa_seguros/polizas/landing/raw"


def test_parseo_de_fechas_en_multiples_formatos(spark):
    df = spark.createDataFrame([("2024-05-06",), ("2024/05/06",), ("06/05/2024",), ("2024-13-40",), (None,)], "v string")
    res = [r.f for r in df.select(parsear_fecha(F.col("v")).alias("f")).collect()]
    assert res[:3] == [date(2024, 5, 6)] * 3  # dd/MM/yyyy se interpreta como día/mes (supuesto documentado)
    assert res[3] is None and res[4] is None


def test_sql_runner_sustituye_variables_y_detecta_faltantes(tmp_path):
    archivo = tmp_path / "x.sql"
    archivo.write_text("-- comentario\nSELECT 1 FROM ${catalog}.${schema}.t;\nSELECT 2;\n", encoding="utf-8")
    assert leer_sentencias(archivo, {"catalog": "c", "schema": "s"}) == ["SELECT 1 FROM c.s.t", "SELECT 2"]
    with pytest.raises(ValueError, match="schema"):
        leer_sentencias(archivo, {"catalog": "c"})
