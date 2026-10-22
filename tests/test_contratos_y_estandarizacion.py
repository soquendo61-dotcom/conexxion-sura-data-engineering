from contracts.definitions import CONTRATOS, POLIZAS
from quality.rules import reglas_para
from transformations.standardize import (
    aplicar_contrato,
    limpiar_textos,
    normalizar_nombre_columna,
    tipar,
    validar_contrato,
)


def test_normalizacion_de_nombres_de_columna():
    assert normalizar_nombre_columna("﻿cliente_id") == "cliente_id"
    assert normalizar_nombre_columna(" Fecha Pago ") == "fecha_pago"
    assert normalizar_nombre_columna("Número-Documento") == "numero_documento"


def test_validacion_de_contrato_detecta_faltantes_adicionales_y_alias():
    columnas = [c for c in POLIZAS.nombres if c not in ("prima", "cliente_id")] + ["valor_prima", "canal_detalle"]
    r = validar_contrato(columnas, POLIZAS)
    assert r.renombradas == {"valor_prima": "prima"}
    assert r.faltantes_requeridas == ["cliente_id"]
    assert r.adicionales == ["canal_detalle"]
    assert not r.cumple


def test_alias_de_esquema_nuevo_y_viejo_en_el_mismo_lote(spark, bronze):
    """Regresión: un archivo trae 'prima' y otro 'valor_prima'; ninguna fila puede perder la prima."""
    base = {"poliza_id": "P1", "cliente_id": "C1", "producto": "Autos", "sucursal": "Cali",
            "fecha_inicio_vigencia": "2026-01-01", "fecha_fin_vigencia": "2027-01-01", "estado": "Vigente",
            "fecha_actualizacion": "2026-01-01"}
    viejo = bronze("polizas", [{**base, "prima": "100.50"}], archivo="polizas_v1.csv")
    nuevo = bronze("polizas", [{**base, "poliza_id": "P2", "valor_prima": "200.25"}], archivo="polizas_v2.csv")
    df = aplicar_contrato(viejo.unionByName(nuevo, allowMissingColumns=True), POLIZAS)
    assert "valor_prima" not in df.columns
    assert {r.poliza_id: r.prima for r in df.collect()} == {"P1": "100.50", "P2": "200.25"}


def test_limpieza_y_tipado_registran_errores_sin_romper(spark, bronze):
    df = bronze("polizas", [{
        "poliza_id": " P1 ", "cliente_id": "C1", "producto": "NULL", "sucursal": "", "fecha_inicio_vigencia": "2026/01/15",
        "fecha_fin_vigencia": "fecha_mala", "estado": "Vigente", "prima": "1,5", "fecha_actualizacion": "15/01/2026",
    }])
    df = tipar(limpiar_textos(aplicar_contrato(df, POLIZAS), POLIZAS.nombres), POLIZAS)
    r = df.collect()[0]
    assert r.poliza_id == "P1" and r.producto is None and r.sucursal is None
    assert str(r.fecha_inicio_vigencia) == "2026-01-15" and str(r.fecha_actualizacion) == "2026-01-15"
    assert r.fecha_fin_vigencia is None and r.prima is None
    assert sorted(r._errores_tipo) == ["fecha_fin_vigencia", "prima"]


def test_cada_dataset_tiene_reglas_de_las_seis_dimensiones_minimas(cfg):
    dimensiones = set()
    for ds in CONTRATOS:
        reglas = reglas_para(ds, cfg)
        assert len({r.id for r in reglas}) == len(reglas), f"IDs de regla repetidos en {ds}"
        dimensiones |= {r.dimension for r in reglas}
    assert dimensiones == {"completitud", "unicidad", "validez", "consistencia", "integridad_referencial", "oportunidad"}
