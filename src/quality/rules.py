"""Catálogo de reglas de calidad por dataset (Silver).

Convención de identificadores: ``<PREFIJO>_<DESCRIPCION>`` (CLI, POL, REC, SIN, MOV).
Los umbrales se calibraron con el perfilamiento de las fuentes (docs/04_perfilamiento_fuentes.md):
reglas de consistencia de vigencia tienen umbrales altos porque el dato dummy presenta una
proporción elevada de pagos/siniestros fuera de vigencia que NO se consideran error bloqueante
sino discrepancia de negocio a investigar (se publica en ``gold_discrepancias``).
"""

from __future__ import annotations

from common.config import PipelineConfig
from contracts.definitions import CONTRATOS, ContratoDataset
from quality.engine import Regla

PREFIJOS = {"clientes": "CLI", "polizas": "POL", "recaudos": "REC", "siniestros": "SIN", "movimientos": "MOV"}


def _r(ds: str, sufijo: str, dimension: str, descripcion: str, expresion: str, severidad: str,
       accion: str, umbral: float) -> Regla:
    return Regla(f"{PREFIJOS[ds]}_{sufijo}", ds, dimension, descripcion, expresion, severidad, accion, umbral)


def reglas_comunes(contrato: ContratoDataset) -> list[Regla]:
    ds, clave = contrato.nombre, contrato.clave
    reglas = [
        _r(ds, "ESTRUCTURA_FILA", "validez", "Fila con estructura válida (no malformada)",
           "_corrupt_record IS NULL", "BLOQUEO", "CUARENTENA", 1.0),
        _r(ds, "PK_NO_NULA", "completitud", f"{clave} informado", f"{clave} IS NOT NULL",
           "BLOQUEO", "CUARENTENA", 1.0),
        _r(ds, "TIPOS_VALIDOS", "validez", "Fechas y montos convertibles al tipo del contrato",
           "size(_errores_tipo) = 0", "BLOQUEO", "CUARENTENA", 2.0),
        _r(ds, "DUP_EXACTO", "unicidad", "Fila no repetida exactamente dentro del mismo archivo",
           "NOT _dup_exacto", "ADVERTENCIA", "CUARENTENA", 5.0),
        _r(ds, "DUP_NEGOCIO", "unicidad", "Mismo hecho no reenviado con otro identificador técnico",
           "NOT _dup_negocio", "ADVERTENCIA", "CUARENTENA", 5.0),
        _r(ds, "EMPATE_VERSION", "consistencia",
           "Sin versiones con contenido distinto y la misma marca temporal (se desempata determinísticamente)",
           "NOT _empate_version", "INFORMATIVA", "MARCAR", 100.0),
    ]
    for col in contrato.columnas:
        if col.dominio:
            valores = ", ".join(f"'{v}'" for v in col.dominio)
            reglas.append(_r(ds, f"{col.nombre.upper()}_DOMINIO", "validez",
                             f"{col.nombre} dentro de los valores permitidos",
                             f"{col.nombre} IS NULL OR {col.nombre} IN ({valores})",
                             "ADVERTENCIA", "CUARENTENA", 2.0))
    return reglas


def _entre(a: str, b: str, cond: str) -> str:
    """La regla solo aplica si ambos campos existen (los nulos los cubre la regla de completitud)."""
    return f"{a} IS NULL OR {b} IS NULL OR ({cond})"


def reglas_especificas(ds: str, cfg: PipelineConfig) -> list[Regla]:
    tardia = cfg.dias_llegada_tardia
    if ds == "clientes":
        return [
            _r(ds, "DOCUMENTO_REQUERIDO", "completitud", "Número de documento informado",
               "numero_documento IS NOT NULL", "ADVERTENCIA", "MARCAR", 5.0),
            _r(ds, "FECHA_ACT_REQUERIDA", "completitud", "Fecha de actualización informada",
               "fecha_actualizacion IS NOT NULL", "ADVERTENCIA", "CUARENTENA", 2.0),
            _r(ds, "NACIMIENTO_VALIDO", "validez", "Fecha de nacimiento entre 1900 y hoy",
               "fecha_nacimiento IS NULL OR fecha_nacimiento BETWEEN DATE'1900-01-01' AND current_date()",
               "ADVERTENCIA", "CUARENTENA", 2.0),
            _r(ds, "CORREO_FORMATO", "validez", "Correo con formato válido",
               "correo IS NULL OR correo RLIKE '^[^@ ]+@[^@ ]+[.][^@ ]+$'", "INFORMATIVA", "MARCAR", 100.0),
            _r(ds, "DOCUMENTO_UNICO", "unicidad", "Documento no compartido por varios clientes",
               "NOT _documento_compartido", "ADVERTENCIA", "MARCAR", 2.0),
        ]
    if ds == "polizas":
        return [
            _r(ds, "CLIENTE_REQUERIDO", "completitud", "Cliente informado", "cliente_id IS NOT NULL",
               "BLOQUEO", "CUARENTENA", 1.0),
            _r(ds, "PRODUCTO_REQUERIDO", "completitud", "Producto informado (si falta se reporta como NO INFORMADO)",
               "producto IS NOT NULL", "ADVERTENCIA", "MARCAR", 2.0),
            _r(ds, "SUCURSAL_REQUERIDA", "completitud", "Sucursal informada", "sucursal IS NOT NULL",
               "ADVERTENCIA", "MARCAR", 2.0),
            _r(ds, "VIGENCIA_REQUERIDA", "completitud", "Inicio y fin de vigencia informados",
               "fecha_inicio_vigencia IS NOT NULL AND fecha_fin_vigencia IS NOT NULL",
               "BLOQUEO", "CUARENTENA", 1.0),
            _r(ds, "PRIMA_POSITIVA", "validez", "Prima informada y mayor que cero", "prima > 0",
               "BLOQUEO", "CUARENTENA", 2.0),
            _r(ds, "VIGENCIA_COHERENTE", "consistencia", "Fin de vigencia posterior o igual al inicio",
               _entre("fecha_inicio_vigencia", "fecha_fin_vigencia", "fecha_fin_vigencia >= fecha_inicio_vigencia"),
               "BLOQUEO", "CUARENTENA", 2.0),
            _r(ds, "DURACION_RAZONABLE", "validez", "Vigencia no mayor a 5 años",
               _entre("fecha_inicio_vigencia", "fecha_fin_vigencia",
                      "datediff(fecha_fin_vigencia, fecha_inicio_vigencia) <= 1830"),
               "ADVERTENCIA", "MARCAR", 2.0),
            _r(ds, "CLIENTE_EXISTE", "integridad_referencial", "El cliente existe en silver_clientes",
               "_fk_cliente_ok", "ADVERTENCIA", "MARCAR", 2.0),
            _r(ds, "MONEDA_COP", "validez", "Moneda COP", "moneda IS NULL OR moneda = 'COP'",
               "INFORMATIVA", "MARCAR", 100.0),
        ]
    if ds == "recaudos":
        return [
            _r(ds, "POLIZA_REQUERIDA", "completitud", "Póliza informada", "poliza_id IS NOT NULL",
               "BLOQUEO", "CUARENTENA", 1.0),
            _r(ds, "FECHA_PAGO_REQUERIDA", "completitud", "Fecha de pago informada", "fecha_pago IS NOT NULL",
               "ADVERTENCIA", "CUARENTENA", 2.0),
            _r(ds, "VALOR_POSITIVO", "validez", "Valor del recaudo mayor que cero", "valor > 0",
               "BLOQUEO", "CUARENTENA", 2.0),
            _r(ds, "POLIZA_EXISTE", "integridad_referencial", "La póliza existe y es válida en silver_polizas",
               "_fk_poliza_ok", "ADVERTENCIA", "MARCAR", 2.0),
            _r(ds, "PAGO_EN_VIGENCIA", "consistencia", "Pago dentro de la vigencia de la póliza",
               "NOT _fk_poliza_ok OR fecha_pago IS NULL OR _pago_en_vigencia", "ADVERTENCIA", "MARCAR", 60.0),
            _r(ds, "INGESTA_POSTERIOR_PAGO", "consistencia", "Fecha de ingesta posterior o igual al pago",
               _entre("fecha_ingesta", "fecha_pago", "fecha_ingesta >= fecha_pago"), "ADVERTENCIA", "MARCAR", 2.0),
            _r(ds, "OPORTUNIDAD", "oportunidad", f"Pago registrado en la fuente máximo {tardia} días después",
               f"dias_rezago_ingesta IS NULL OR dias_rezago_ingesta <= {tardia}", "INFORMATIVA", "MARCAR", 100.0),
        ]
    if ds == "siniestros":
        return [
            _r(ds, "POLIZA_REQUERIDA", "completitud", "Póliza informada", "poliza_id IS NOT NULL",
               "BLOQUEO", "CUARENTENA", 1.0),
            _r(ds, "ESTADO_REQUERIDO", "completitud", "Estado del siniestro informado", "estado IS NOT NULL",
               "ADVERTENCIA", "MARCAR", 2.0),
            _r(ds, "PAGADO_NO_NEGATIVO", "validez", "Valor pagado mayor o igual a cero", "valor_pagado >= 0",
               "BLOQUEO", "CUARENTENA", 2.0),
            _r(ds, "RESERVA_NO_NEGATIVA", "validez", "Reserva mayor o igual a cero", "valor_reserva >= 0",
               "BLOQUEO", "CUARENTENA", 2.0),
            _r(ds, "AVISO_POSTERIOR_OCURRENCIA", "consistencia", "Aviso posterior o igual a la ocurrencia",
               _entre("fecha_aviso", "fecha_ocurrencia", "fecha_aviso >= fecha_ocurrencia"),
               "ADVERTENCIA", "CUARENTENA", 2.0),
            _r(ds, "POLIZA_EXISTE", "integridad_referencial", "La póliza existe y es válida en silver_polizas",
               "_fk_poliza_ok", "ADVERTENCIA", "MARCAR", 2.0),
            _r(ds, "OCURRENCIA_EN_VIGENCIA", "consistencia", "Ocurrencia dentro de la vigencia de la póliza",
               "NOT _fk_poliza_ok OR fecha_ocurrencia IS NULL OR _ocurrencia_en_vigencia",
               "ADVERTENCIA", "MARCAR", 90.0),
            _r(ds, "OPORTUNIDAD_AVISO", "oportunidad", "Aviso máximo 30 días después de la ocurrencia",
               "dias_aviso IS NULL OR dias_aviso <= 30", "INFORMATIVA", "MARCAR", 100.0),
        ]
    if ds == "movimientos":
        return [
            _r(ds, "ID_ENTIDAD_REQUERIDO", "completitud", "Entidad e identificador informados",
               "entidad IS NOT NULL AND id_entidad IS NOT NULL", "BLOQUEO", "CUARENTENA", 1.0),
            _r(ds, "FECHA_EVENTO_REQUERIDA", "completitud", "Fecha del evento informada",
               "fecha_evento IS NOT NULL", "ADVERTENCIA", "CUARENTENA", 2.0),
            _r(ds, "PAYLOAD_VALIDO", "validez", "Payload JSON válido", "_payload_valido",
               "ADVERTENCIA", "CUARENTENA", 2.0),
            _r(ds, "EVENTO_COHERENTE", "consistencia", "Tipo de evento válido para la entidad",
               "_evento_coherente", "ADVERTENCIA", "CUARENTENA", 2.0),
            _r(ds, "ENTIDAD_EXISTE", "integridad_referencial", "La entidad referenciada existe en Silver",
               "_fk_entidad_ok", "ADVERTENCIA", "MARCAR", 2.0),
            _r(ds, "INGESTA_POSTERIOR_EVENTO", "consistencia", "Ingesta posterior o igual al evento",
               _entre("fecha_ingesta", "fecha_evento", "fecha_ingesta >= fecha_evento"),
               "ADVERTENCIA", "MARCAR", 2.0),
            _r(ds, "LLEGADA_TARDIA", "oportunidad", f"Evento recibido máximo {tardia} días después de ocurrido",
               "NOT _llegada_tardia", "INFORMATIVA", "MARCAR", 100.0),
        ]
    raise ValueError(f"Dataset sin reglas definidas: {ds}")


def reglas_para(ds: str, cfg: PipelineConfig) -> list[Regla]:
    return reglas_comunes(CONTRATOS[ds]) + reglas_especificas(ds, cfg)
