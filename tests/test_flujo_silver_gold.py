"""Prueba de integración en memoria: Bronze sintético -> Silver -> Gold con resultados calculados a mano.

Caso (corte 2026-08-31): la póliza P1 vale 365 por 365 días (1 por día) para verificar el devengo exacto.
"""

from datetime import date
from decimal import Decimal

import pytest

from contracts.definitions import CLIENTES, ORDEN_PROCESAMIENTO
from quality import engine
from transformations import gold
from transformations.scd2 import construir_scd2
from transformations.silver import clasificar_versiones, construir_silver, preparar, reconciliar_bronze_silver


def _pol(pid, cli, prod, ini, fin, estado, prima, act="2026-01-01"):
    return {"poliza_id": pid, "cliente_id": cli, "producto": prod, "sucursal": "Cali",
            "fecha_inicio_vigencia": ini, "fecha_fin_vigencia": fin, "estado": estado, "prima": prima,
            "moneda": "COP", "canal": "Directo", "fecha_actualizacion": act, "origen": "CORE"}


def _rec(rid, pid, fecha, valor, estado="Aplicado"):
    return {"recaudo_id": rid, "poliza_id": pid, "fecha_pago": fecha, "valor": valor, "medio_pago": "PSE",
            "estado_transaccion": estado, "referencia_bancaria": f"RB{rid}", "fecha_ingesta": fecha, "origen": "PAGOS"}


def _sin(sid, pid, ocurrencia, aviso, pagado, reserva):
    return {"siniestro_id": sid, "poliza_id": pid, "fecha_ocurrencia": ocurrencia, "fecha_aviso": aviso,
            "tipo_siniestro": "Danos", "estado": "Abierto", "valor_pagado": pagado, "valor_reserva": reserva,
            "ciudad_ocurrencia": "Cali", "fecha_actualizacion": aviso, "origen": "CORE"}


DATOS = {
    "clientes": [
        {"cliente_id": c, "tipo_documento": "CC", "numero_documento": d, "fecha_nacimiento": "1980-01-01",
         "ciudad": "Cali", "segmento": "Personas", "correo": f"{c}@x.com", "telefono": "300",
         "fecha_actualizacion": "2025-01-01", "origen": "CRM"} for c, d in (("C1", "1"), ("C2", "2"))
    ],
    "polizas": [
        _pol("P1", "C1", "Autos", "2026-01-01", "2027-01-01", "Vigente", "365.00"),
        _pol("P2", "C2", "Vida", "2025-09-15", "2026-09-15", "Vigente", "100.00"),
        _pol("P3", "C2", "Hogar", "2025-05-01", "2026-05-01", "Vencida", "100.00"),
        _pol("P4", "C1", "Salud", "2026-01-01", "2027-01-01", "Cancelada", "100.00", act="2026-02-01"),
        _pol("P5", "CX", "Hogar", "2026-01-01", "2027-01-01", "Vigente", "50.00"),       # cliente inexistente
        _pol("P6", "C1", "Autos", "2026-01-01", "2027-01-01", "Vigente", "-10.00"),      # prima negativa
    ],
    "recaudos": [
        _rec("R1", "P1", "2026-02-01", "100.00"),
        _rec("R2", "P1", "2026-03-01", "50.00", "Pendiente"),
        _rec("R3", "P1", "2026-09-10", "30.00"),                 # posterior al corte: no cuenta
        _rec("R4", "PX", "2026-04-01", "70.00"),                 # sin póliza
        _rec("R5", "P1", "2026-04-01", "-5.00"),                 # negativo: cuarentena
    ],
    "siniestros": [
        _sin("S1", "P1", "2026-03-10", "2026-03-12", "10.00", "5.00"),
        _sin("S2", "P1", "2026-08-30", "2026-09-02", "99.00", "1.00"),   # avisado después del corte
        _sin("S3", "PX", "2026-03-10", "2026-03-12", "20.00", "0.00"),   # sin póliza válida
        _sin("S4", "P6", "2026-03-10", "2026-03-12", "20.00", "0.00"),   # su póliza quedó en cuarentena
    ],
    "movimientos": [
        {"evento_id": "E1", "entidad": "POLIZA", "id_entidad": "P1", "tipo_evento": "CAMBIO_ESTADO",
         "fecha_evento": "2026-08-20", "fecha_ingesta": "2026-08-21",
         "payload": '{"estado_origen":"NA","estado_destino":"Vigente","usuario_proceso":"p1"}', "secuencia": "1",
         "origen": "BUS"},
        {"evento_id": "E2", "entidad": "SINIESTRO", "id_entidad": "S9", "tipo_evento": "AVISO",
         "fecha_evento": "2026-05-01", "fecha_ingesta": "2026-05-01", "payload": None, "secuencia": "1",
         "origen": "BUS"},
    ],
}


@pytest.fixture(scope="module")
def resultado(spark, request):
    from common.config import PipelineConfig

    cfg = PipelineConfig.desde_parametros({"fecha_corte": "2026-08-31", "run_id": "test"})
    construir_bronze = request.getfixturevalue("bronze")
    silver, controles, salidas = {}, [], {}
    for ds in ORDEN_PROCESAMIENTO:
        b = construir_bronze(ds, DATOS[ds], archivo=f"{ds}.csv")
        salida = construir_silver(b, ds, silver, cfg)
        silver[ds], salidas[ds] = salida.aceptados, salida
        controles += engine.resumir(salida.evaluado, salida.reglas) + reconciliar_bronze_silver(b, salida, ds)
    p360 = gold.construir_poliza_360(silver, cfg)
    sm = gold.construir_siniestralidad_mensual(silver, cfg)
    return {
        "cfg": cfg, "silver": silver, "salidas": salidas, "controles": controles,
        "p360": {r.poliza_id: r for r in p360.collect()}, "p360_df": p360, "sm_df": sm,
        "sm": {(r.periodo, r.producto): r for r in sm.collect()},
        "disc": gold.construir_discrepancias(silver, p360, cfg).collect(),
    }


def test_silver_cuarentena_y_reconciliacion(resultado):
    cuarentena = {(r.dataset, r.clave_negocio, r.regla_id) for s in resultado["salidas"].values()
                  for r in s.cuarentena.collect()}
    assert ("polizas", "P6", "POL_PRIMA_POSITIVA") in cuarentena
    assert ("recaudos", "R5", "REC_VALOR_POSITIVO") in cuarentena
    recon = [c for c in resultado["controles"] if c["tipo_control"] == "RECONCILIACION"]
    assert recon and all(c["resultado"] == "OK" for c in recon), recon
    # Huérfanos: se conservan en Silver marcados (no se descartan) para reportarlos como discrepancia.
    rec = {r.recaudo_id: r for r in resultado["silver"]["recaudos"].collect()}
    assert rec["R4"]._fk_poliza_ok is False and "REC_POLIZA_EXISTE" in rec["R4"]._dq_advertencias


def test_estado_al_corte(resultado):
    estados = {k: v.estado_corte for k, v in resultado["p360"].items()}
    assert estados == {"P1": "VIGENTE", "P2": "PROXIMA_A_VENCER", "P3": "VENCIDA", "P4": "CANCELADA", "P5": "VIGENTE"}
    assert resultado["p360"]["P5"].cliente_existe is False


def test_poliza_360_metricas_y_ultima_actividad(resultado):
    p1 = resultado["p360"]["P1"]
    assert p1.recaudo_aplicado == Decimal("100.00")      # R3 es posterior al corte y R5 está en cuarentena
    assert p1.recaudo_pendiente == Decimal("50.00")
    assert p1.saldo_por_recaudar == Decimal("265.00")
    assert p1.num_siniestros == 1 and p1.costo_incurrido == Decimal("15.00")   # S2 avisado después del corte
    assert (p1.ultima_actividad_fecha, p1.ultima_actividad_origen) == (date(2026, 8, 20), "EVENTO")
    assert p1.ultimo_evento == "POLIZA:CAMBIO_ESTADO"


def test_siniestralidad_mensual_devengo_prorrata(resultado):
    autos = {k[0]: v for k, v in resultado["sm"].items() if k[1] == "Autos"}
    assert sum(v.prima_devengada for v in autos.values()) == Decimal("243.00")   # 1 ene - 31 ago = 243 días
    marzo = autos["2026-03"]
    assert marzo.prima_devengada == Decimal("31.00") and marzo.polizas_expuestas == 1
    assert marzo.costo_incurrido == Decimal("15.00")
    assert marzo.razon_siniestralidad == Decimal("0.4839")   # 15 / 31
    # Cancelada: deja de devengar en la fecha efectiva de cancelación (2026-02-01): solo enero.
    salud = {k[0]: v for k, v in resultado["sm"].items() if k[1] == "Salud"}
    assert salud["2026-01"].prima_devengada > 0 and "2026-03" not in salud


def test_discrepancias_reconciliacion_y_postcondiciones_gold(resultado):
    tipos = {(r.tipo_discrepancia, r.id_registro) for r in resultado["disc"]}
    assert ("RECAUDO_SIN_POLIZA_VALIDA", "R4") in tipos
    assert ("SINIESTRO_SIN_POLIZA_VALIDA", "S3") in tipos
    assert ("SINIESTRO_SIN_POLIZA_VALIDA", "S4") in tipos      # póliza rechazada => sin póliza válida
    assert ("POLIZA_SIN_CLIENTE", "P5") in tipos
    assert ("MOVIMIENTO_ENTIDAD_INEXISTENTE", "E2") in tipos
    cfg = resultado["cfg"]
    controles = gold.reconciliar_silver_gold(resultado["silver"], resultado["p360_df"], resultado["sm_df"], cfg) \
        + gold.postcondiciones(resultado["p360_df"], resultado["sm_df"], cfg)
    assert all(c["resultado"] == "OK" for c in controles), [c for c in controles if c["resultado"] != "OK"]


def test_scd2_clientes_vigencias_y_colapso(spark, bronze):
    base = DATOS["clientes"][0]
    df = bronze("clientes", [
        {**base, "fecha_actualizacion": "2026-01-01", "ciudad": "Cali"},
        {**base, "fecha_actualizacion": "2026-03-01", "ciudad": "Medellin"},
        {**base, "fecha_actualizacion": "2026-05-01", "ciudad": "Medellin"},   # sin cambios: no crea versión
    ])
    versiones = clasificar_versiones(preparar(df, CLIENTES), CLIENTES)
    atributos = [c for c in CLIENTES.nombres if c not in ("fecha_actualizacion", "origen")]
    filas = sorted(construir_scd2(versiones, "cliente_id", "fecha_actualizacion", atributos).collect(),
                   key=lambda r: r.version)
    assert [(r.ciudad, str(r.valido_desde), str(r.valido_hasta), r.es_actual) for r in filas] == [
        ("Cali", "2026-01-01", "2026-02-28", False),
        ("Medellin", "2026-03-01", "9999-12-31", True),
    ]
