from datetime import datetime

from pyspark.sql import functions as F

from contracts.definitions import CLIENTES
from quality import engine
from quality.engine import Regla
from transformations.silver import clasificar_versiones, preparar


def _cliente(cid, doc="100", act="2026-01-01", ciudad="Cali"):
    return {"cliente_id": cid, "tipo_documento": "CC", "numero_documento": doc, "fecha_nacimiento": "1990-01-01",
            "ciudad": ciudad, "segmento": "Personas", "correo": f"{cid}@x.com", "telefono": "300",
            "fecha_actualizacion": act, "origen": "CRM"}


def _clasificar(df):
    return {r._record_id: r for r in clasificar_versiones(preparar(df, CLIENTES), CLIENTES).collect()}


def test_duplicado_exacto_solo_dentro_del_mismo_archivo(spark, bronze):
    f1 = bronze("clientes", [_cliente("C1"), _cliente("C1")], archivo="clientes_1.csv")
    f2 = bronze("clientes", [_cliente("C1")], archivo="clientes_2.csv", modificacion=datetime(2026, 10, 1))
    filas = list(_clasificar(f1.unionByName(f2)).values())
    assert sum(r._dup_exacto for r in filas) == 1          # la repetición en el mismo archivo
    actuales = [r for r in filas if r._es_version_actual]
    assert len(actuales) == 1 and actuales[0]._source_file.endswith("clientes_2.csv")  # reenvío = nueva versión


def test_version_mas_reciente_y_empate_de_marca_temporal(spark, bronze):
    df = bronze("clientes", [
        _cliente("C1", act="2026-01-01", ciudad="Cali"),
        _cliente("C1", act="2026-03-01", ciudad="Medellin"),
        _cliente("C1", act="2026-03-01", ciudad="Bogota"),   # misma marca temporal, contenido distinto
    ])
    filas = list(_clasificar(df).values())
    actual = [r for r in filas if r._es_version_actual]
    assert len(actual) == 1 and str(actual[0].fecha_actualizacion) == "2026-03-01"
    assert sum(r._empate_version for r in filas) == 2
    # El desempate es determinístico: se repite el mismo ganador en otra evaluación.
    assert [r for r in _clasificar(df).values() if r._es_version_actual][0].ciudad == actual[0].ciudad


def test_duplicado_de_negocio_conserva_el_identificador_original(spark, bronze):
    original = _cliente("C1", doc="555")
    reenvio = {**original, "cliente_id": "C9"}            # mismo hecho, otro identificador técnico
    distinto = _cliente("C5", doc="777")
    filas = {r.cliente_id: r for r in _clasificar(bronze("clientes", [reenvio, original, distinto])).values()}
    assert filas["C9"]._dup_negocio and filas["C9"]._id_original == "C1"
    assert not filas["C1"]._dup_negocio and not filas["C5"]._dup_negocio


def test_motor_de_reglas_cuarentena_marcas_y_umbrales(spark):
    df = spark.createDataFrame(
        [("A", 10, "x"), ("B", -5, "x"), ("C", None, "x"), ("D", 7, None)],
        "id string, valor int, campo string",
    ).withColumns({"_registro_original": F.lit("{}"), "_source_file": F.lit("f"), "_file_hash": F.lit("h"),
                   "_row_hash": F.lit("r"), "_record_id": F.col("id")})
    reglas = [
        Regla("T_VALOR_POSITIVO", "t", "validez", "valor > 0", "valor > 0", "BLOQUEO", "CUARENTENA", 25.0),
        Regla("T_CAMPO", "t", "completitud", "campo informado", "campo IS NOT NULL", "ADVERTENCIA", "MARCAR", 10.0),
    ]
    evaluado = engine.evaluar_reglas(df, reglas)
    aceptados = {r.id: r._dq_advertencias for r in engine.aceptados(evaluado, reglas).collect()}
    assert aceptados == {"A": [], "D": ["T_CAMPO"]}          # null en la expresión = incumplimiento
    cuarentena = engine.construir_cuarentena(evaluado, reglas, dataset="t", clave="id", run_id="r",
                                             pipeline="p", fecha_corte="2026-08-31").collect()
    assert sorted(r.clave_negocio for r in cuarentena) == ["B", "C"]
    assert {r.regla_id for r in cuarentena} == {"T_VALOR_POSITIVO"}
    resumen = {r["regla_id"]: r for r in engine.resumir(evaluado, reglas)}
    assert resumen["T_VALOR_POSITIVO"]["pct_fallo"] == 50.0
    assert resumen["T_VALOR_POSITIVO"]["resultado"] == "FALLA"      # 50% > umbral 25% en regla BLOQUEO
    assert resumen["T_CAMPO"]["resultado"] == "ADVERTENCIA"         # 25% > 10%
    assert [b["regla_id"] for b in engine.bloqueos(list(resumen.values()))] == ["T_VALOR_POSITIVO"]
