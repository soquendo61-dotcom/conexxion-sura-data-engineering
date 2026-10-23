"""Productos de datos Gold (funciones puras, una foto por ``fecha_corte``).

Reglas de negocio (ver docs/05_modelo_datos_y_calidad.md):
- Solo se usan registros aceptados en Silver. Hijos sin póliza válida se excluyen de las
  métricas y se publican en ``gold_discrepancias``.
- Corte (point-in-time por fecha de negocio): pólizas con inicio <= corte; recaudos con
  fecha_pago <= corte; siniestros con fecha_aviso <= corte (lo conocido al corte);
  movimientos con fecha_evento <= corte.
- Estado al corte: CANCELADA > (fin < corte: RENOVADA | VENCIDA) > NO_INICIADA >
  PROXIMA_A_VENCER (fin dentro de N días) > VIGENTE.
- Prima devengada: prorrata diaria de la prima sobre [inicio, fin) acotada al mes, al corte y,
  para canceladas, a la fecha efectiva de cancelación (fecha_actualizacion dentro de vigencia).
- Costo incurrido = valor pagado + reserva. Razón de siniestralidad = costo incurrido / prima devengada.
"""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from common.config import PipelineConfig

ESTADOS_CORTE = ("VIGENTE", "PROXIMA_A_VENCER", "VENCIDA", "RENOVADA", "CANCELADA", "NO_INICIADA")
DEC = "decimal(18,2)"


def _corte(cfg: PipelineConfig) -> Column:
    return F.lit(cfg.fecha_corte_str).cast("date")


def _sum(col: Column) -> Column:
    return F.coalesce(F.sum(col), F.lit(0)).cast(DEC)


def estado_al_corte(cfg: PipelineConfig) -> Column:
    corte = _corte(cfg)
    ini, fin, est = F.col("fecha_inicio_vigencia"), F.col("fecha_fin_vigencia"), F.col("estado")
    return (
        F.when(est == "Cancelada", "CANCELADA")
        .when(fin < corte, F.when(est == "Renovada", "RENOVADA").otherwise("VENCIDA"))
        .when(ini > corte, "NO_INICIADA")
        .when(fin <= F.date_add(corte, cfg.dias_proxima_a_vencer), "PROXIMA_A_VENCER")
        .otherwise("VIGENTE")
    )


def fin_efectiva() -> Column:
    """Para canceladas se usa fecha_actualizacion como fecha efectiva si cae dentro de la vigencia."""
    act = F.col("fecha_actualizacion")
    return F.when(
        (F.col("estado") == "Cancelada") & act.between(F.col("fecha_inicio_vigencia"), F.col("fecha_fin_vigencia")),
        act,
    ).otherwise(F.col("fecha_fin_vigencia"))


def polizas_al_corte(silver: dict[str, DataFrame], cfg: PipelineConfig) -> DataFrame:
    return silver["polizas"].filter(F.col("fecha_inicio_vigencia") <= _corte(cfg))


def _recaudos_validos(silver, cfg) -> DataFrame:
    return silver["recaudos"].filter(F.col("_fk_poliza_ok") & (F.col("fecha_pago") <= _corte(cfg)))


def _siniestros_validos(silver, cfg) -> DataFrame:
    return silver["siniestros"].filter(F.col("_fk_poliza_ok") & (F.col("fecha_aviso") <= _corte(cfg)))


# =========================================================================== gold_poliza_360
def construir_poliza_360(silver: dict[str, DataFrame], cfg: PipelineConfig) -> DataFrame:
    corte = _corte(cfg)
    pol = polizas_al_corte(silver, cfg)
    clientes = silver["clientes"].select(
        "cliente_id",
        F.col("segmento").alias("cliente_segmento"),
        F.col("ciudad").alias("cliente_ciudad"),
        F.col("tipo_documento").alias("cliente_tipo_documento"),
    )
    estado_tx = F.col("estado_transaccion")
    recaudos = _recaudos_validos(silver, cfg).groupBy("poliza_id").agg(
        _sum(F.when(estado_tx == "Aplicado", F.col("valor"))).alias("recaudo_aplicado"),
        _sum(F.when(estado_tx == "Pendiente", F.col("valor"))).alias("recaudo_pendiente"),
        _sum(F.when(estado_tx == "Reversado", F.col("valor"))).alias("recaudo_reversado"),
        _sum(F.when(estado_tx == "Rechazado", F.col("valor"))).alias("recaudo_rechazado"),
        F.count(F.lit(1)).alias("num_recaudos"),
        F.sum(F.when(~F.col("_pago_en_vigencia"), 1).otherwise(0)).alias("num_recaudos_fuera_vigencia"),
        F.max("fecha_pago").alias("ultima_fecha_pago"),
    )
    siniestros = _siniestros_validos(silver, cfg).groupBy("poliza_id").agg(
        F.count(F.lit(1)).alias("num_siniestros"),
        F.sum(F.col("siniestro_abierto").cast("int")).alias("num_siniestros_abiertos"),
        _sum(F.col("valor_pagado")).alias("siniestros_valor_pagado"),
        _sum(F.col("valor_reserva")).alias("siniestros_valor_reserva"),
        _sum(F.col("costo_incurrido")).alias("costo_incurrido"),
        F.sum(F.when(~F.col("_ocurrencia_en_vigencia"), 1).otherwise(0)).alias("num_siniestros_fuera_vigencia"),
        F.max(F.when(F.col("fecha_actualizacion") <= corte, F.col("fecha_actualizacion"))
              .otherwise(F.col("fecha_aviso"))).alias("ultima_fecha_siniestro"),
    )
    orden_evento = F.struct("fecha_evento", "secuencia", "fecha_ingesta", "evento_id")
    eventos = (
        silver["movimientos"]
        .filter(F.col("poliza_id_relacionada").isNotNull() & (F.col("fecha_evento") <= corte))
        .groupBy(F.col("poliza_id_relacionada").alias("poliza_id"))
        .agg(
            F.count(F.lit(1)).alias("num_eventos"),
            F.max("fecha_evento").alias("ultima_fecha_evento"),
            F.max_by(F.concat_ws(":", "entidad", "tipo_evento"), orden_evento).alias("ultimo_evento"),
        )
    )

    df = (
        pol.join(F.broadcast(clientes), "cliente_id", "left")
        .join(recaudos, "poliza_id", "left")
        .join(siniestros, "poliza_id", "left")
        .join(eventos, "poliza_id", "left")
    )
    ceros = ["recaudo_aplicado", "recaudo_pendiente", "recaudo_reversado", "recaudo_rechazado",
             "siniestros_valor_pagado", "siniestros_valor_reserva", "costo_incurrido"]
    conteos = ["num_recaudos", "num_recaudos_fuera_vigencia", "num_siniestros", "num_siniestros_abiertos",
               "num_siniestros_fuera_vigencia", "num_eventos"]
    df = df.fillna(0, subset=ceros + conteos)

    candidatos = [
        ("POLIZA", F.when(F.col("fecha_actualizacion") <= corte, F.col("fecha_actualizacion"))),
        ("RECAUDO", F.col("ultima_fecha_pago")),
        ("SINIESTRO", F.col("ultima_fecha_siniestro")),
        ("EVENTO", F.col("ultima_fecha_evento")),
    ]
    ultima = F.array_max(F.array_compact(F.array(*[
        F.when(fecha.isNotNull(), F.struct(fecha.alias("fecha"), F.lit(origen).alias("origen")))
        for origen, fecha in candidatos
    ])))
    estado_corte = estado_al_corte(cfg)
    esperado_fuente = (
        F.when(estado_corte.isin("VIGENTE", "PROXIMA_A_VENCER"), "Vigente")
        .when(estado_corte == "VENCIDA", "Vencida")
        .when(estado_corte == "RENOVADA", "Renovada")
        .when(estado_corte == "CANCELADA", "Cancelada")
    )
    return (
        df.withColumn("_ultima", ultima)
        .select(
            corte.alias("fecha_corte"),
            "poliza_id",
            "cliente_id",
            F.col("_fk_cliente_ok").alias("cliente_existe"),
            "cliente_segmento", "cliente_ciudad", "cliente_tipo_documento",
            F.coalesce("producto", F.lit("NO INFORMADO")).alias("producto"),
            F.coalesce("sucursal", F.lit("NO INFORMADA")).alias("sucursal"),
            "canal",
            "fecha_inicio_vigencia", "fecha_fin_vigencia",
            fin_efectiva().alias("fecha_fin_efectiva"),
            "dias_vigencia",
            F.col("estado").alias("estado_fuente"),
            estado_corte.alias("estado_corte"),
            (F.col("estado") == esperado_fuente).alias("estado_consistente"),
            F.when(estado_corte.isin("VIGENTE", "PROXIMA_A_VENCER"),
                   F.datediff("fecha_fin_vigencia", corte)).alias("dias_para_vencer"),
            F.col("prima").alias("prima_emitida"),
            *ceros[:4],
            (F.col("prima") - F.col("recaudo_aplicado")).cast(DEC).alias("saldo_por_recaudar"),
            F.round(F.col("recaudo_aplicado") / F.col("prima") * 100, 2).alias("pct_recaudo"),
            (F.col("recaudo_aplicado") > F.col("prima") * 1.05).alias("sobre_recaudo"),
            "num_recaudos", "num_recaudos_fuera_vigencia", "ultima_fecha_pago",
            "num_siniestros", "num_siniestros_abiertos", "siniestros_valor_pagado",
            "siniestros_valor_reserva", "costo_incurrido", "num_siniestros_fuera_vigencia",
            "num_eventos", "ultimo_evento", "ultima_fecha_evento",
            F.col("_ultima.fecha").alias("ultima_actividad_fecha"),
            F.col("_ultima.origen").alias("ultima_actividad_origen"),
            F.col("_dq_advertencias").alias("advertencias_calidad"),
            F.lit(cfg.run_id).alias("_run_id"),
            F.current_timestamp().alias("_gold_ts"),
        )
    )


# =================================================================== gold_siniestralidad_mensual
def construir_siniestralidad_mensual(silver: dict[str, DataFrame], cfg: PipelineConfig) -> DataFrame:
    corte = _corte(cfg)
    mes_corte = F.trunc(corte, "month")
    dims = ["periodo", "producto", "sucursal"]
    pol = polizas_al_corte(silver, cfg).select(
        "poliza_id",
        F.coalesce("producto", F.lit("NO INFORMADO")).alias("producto"),
        F.coalesce("sucursal", F.lit("NO INFORMADA")).alias("sucursal"),
        "fecha_inicio_vigencia", "fecha_fin_vigencia", "prima",
        fin_efectiva().alias("fin_ef"),
        F.greatest(F.datediff("fecha_fin_vigencia", "fecha_inicio_vigencia"), F.lit(1)).alias("dias_totales"),
    )

    # Exposición y prima devengada: una fila por póliza y mes de vigencia hasta el corte.
    mes_ini = F.trunc("fecha_inicio_vigencia", "month")
    mes_fin = F.trunc(F.least(F.col("fin_ef"), corte), "month")
    exposicion = (
        pol.filter(mes_fin >= mes_ini)
        .withColumn("periodo", F.explode(F.sequence(mes_ini, mes_fin, F.expr("INTERVAL 1 MONTH"))))
        .withColumn("dias_mes", F.greatest(
            F.datediff(
                F.least(F.col("fin_ef"), F.add_months("periodo", 1), F.date_add(corte, 1)),
                F.greatest(F.col("fecha_inicio_vigencia"), F.col("periodo")),
            ), F.lit(0)))
        .groupBy(*dims)
        .agg(
            F.countDistinct(F.when(F.col("dias_mes") > 0, F.col("poliza_id"))).alias("polizas_expuestas"),
            F.sum("dias_mes").alias("dias_exposicion"),
            _sum(F.col("prima") * F.col("dias_mes") / F.col("dias_totales")).alias("prima_devengada"),
        )
    )
    emision = (
        pol.groupBy(F.trunc("fecha_inicio_vigencia", "month").alias("periodo"), "producto", "sucursal")
        .agg(F.count(F.lit(1)).alias("polizas_emitidas"), _sum(F.col("prima")).alias("prima_emitida"))
    )
    dim_pol = pol.select("poliza_id", "producto", "sucursal")
    recaudo = (
        _recaudos_validos(silver, cfg).join(dim_pol, "poliza_id")
        .groupBy(F.trunc("fecha_pago", "month").alias("periodo"), "producto", "sucursal")
        .agg(
            _sum(F.when(F.col("estado_transaccion") == "Aplicado", F.col("valor"))).alias("prima_recaudada"),
            _sum(F.when(F.col("estado_transaccion") == "Pendiente", F.col("valor"))).alias("recaudo_pendiente"),
        )
    )
    siniestros = (
        _siniestros_validos(silver, cfg).join(dim_pol, "poliza_id")
        .groupBy(F.trunc("fecha_ocurrencia", "month").alias("periodo"), "producto", "sucursal")
        .agg(
            F.count(F.lit(1)).alias("num_siniestros"),
            _sum(F.col("valor_pagado")).alias("valor_pagado"),
            _sum(F.col("valor_reserva")).alias("valor_reserva"),
            _sum(F.col("costo_incurrido")).alias("costo_incurrido"),
        )
    )
    df = (
        exposicion.join(emision, dims, "full")
        .join(recaudo, dims, "full")
        .join(siniestros, dims, "full")
        .filter(F.col("periodo") <= mes_corte)
    )
    metricas = ["polizas_expuestas", "dias_exposicion", "prima_devengada", "polizas_emitidas", "prima_emitida",
                "prima_recaudada", "recaudo_pendiente", "num_siniestros", "valor_pagado", "valor_reserva",
                "costo_incurrido"]
    df = df.fillna(0, subset=metricas)
    w = Window.partitionBy("producto", "sucursal").orderBy("periodo").rowsBetween(Window.unboundedPreceding, 0)
    return df.select(
        corte.alias("fecha_corte"),
        F.date_format("periodo", "yyyy-MM").alias("periodo"),
        F.col("periodo").alias("fecha_periodo"),
        "producto", "sucursal",
        *metricas,
        F.when(F.col("prima_devengada") > 0,
               F.round(F.col("costo_incurrido") / F.col("prima_devengada"), 4)).alias("razon_siniestralidad"),
        (F.sum("prima_emitida").over(w) - F.sum("prima_recaudada").over(w)).cast(DEC)
        .alias("prima_pendiente_acumulada"),
        F.lit(cfg.run_id).alias("_run_id"),
        F.current_timestamp().alias("_gold_ts"),
    )


# =========================================================================== gold_discrepancias
def construir_discrepancias(silver: dict[str, DataFrame], poliza_360: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """Discrepancias de negocio entre recaudos, pólizas, siniestros y movimientos al corte."""
    corte = _corte(cfg)

    def fila(df: DataFrame, tipo: str, severidad: str, entidad: str, id_col: str, poliza: Column,
             fecha: Column, valor: Column, detalle: Column) -> DataFrame:
        return df.select(
            corte.alias("fecha_corte"), F.lit(tipo).alias("tipo_discrepancia"),
            F.lit(severidad).alias("severidad"), F.lit(entidad).alias("entidad"),
            F.col(id_col).cast("string").alias("id_registro"), poliza.cast("string").alias("poliza_id"),
            fecha.cast("date").alias("fecha_referencia"), valor.cast(DEC).alias("valor"),
            detalle.alias("detalle"),
        )

    pol = polizas_al_corte(silver, cfg)
    rec = silver["recaudos"].filter(F.col("fecha_pago") <= corte)
    sin = silver["siniestros"].filter(F.col("fecha_aviso") <= corte)
    mov = silver["movimientos"].filter(F.col("fecha_evento") <= corte)
    vig = pol.select(F.col("poliza_id").alias("__p"), F.col("fecha_inicio_vigencia").alias("__i"),
                     F.col("fecha_fin_vigencia").alias("__f"))
    mov_pol = (
        mov.filter(F.col("entidad") == "POLIZA")
        .join(vig, F.col("id_entidad") == F.col("__p"))
        .filter(~F.col("fecha_evento").between(F.col("__i"), F.col("__f")))
    )
    partes = [
        fila(pol.filter(~F.col("_fk_cliente_ok")), "POLIZA_SIN_CLIENTE", "ALTA", "POLIZA", "poliza_id",
             F.col("poliza_id"), F.col("fecha_inicio_vigencia"), F.col("prima"),
             F.concat(F.lit("cliente_id inexistente: "), F.col("cliente_id"))),
        fila(rec.filter(~F.col("_fk_poliza_ok")), "RECAUDO_SIN_POLIZA_VALIDA", "ALTA", "RECAUDO", "recaudo_id",
             F.col("poliza_id"), F.col("fecha_pago"), F.col("valor"),
             F.concat(F.lit("Pago sin correspondencia con póliza válida; estado="), F.col("estado_transaccion"))),
        fila(sin.filter(~F.col("_fk_poliza_ok")), "SINIESTRO_SIN_POLIZA_VALIDA", "ALTA", "SINIESTRO",
             "siniestro_id", F.col("poliza_id"), F.col("fecha_ocurrencia"), F.col("costo_incurrido"),
             F.concat(F.lit("Siniestro sin póliza válida; estado="), F.coalesce("estado", F.lit("NULL")))),
        fila(rec.filter(F.col("_fk_poliza_ok") & ~F.col("_pago_en_vigencia")), "RECAUDO_FUERA_VIGENCIA", "MEDIA",
             "RECAUDO", "recaudo_id", F.col("poliza_id"), F.col("fecha_pago"), F.col("valor"),
             F.lit("Pago registrado fuera de la vigencia de la póliza")),
        fila(sin.filter(F.col("_fk_poliza_ok") & ~F.col("_ocurrencia_en_vigencia")), "SINIESTRO_FUERA_VIGENCIA",
             "ALTA", "SINIESTRO", "siniestro_id", F.col("poliza_id"), F.col("fecha_ocurrencia"),
             F.col("costo_incurrido"), F.lit("Ocurrencia fuera de la vigencia de la póliza (validar cobertura)")),
        fila(mov.filter(~F.col("_fk_entidad_ok")), "MOVIMIENTO_ENTIDAD_INEXISTENTE", "MEDIA", "MOVIMIENTO",
             "evento_id", F.col("poliza_id_relacionada"), F.col("fecha_evento"), F.lit(None),
             F.concat_ws(" ", F.lit("Evento sobre entidad inexistente:"), "entidad", "id_entidad", "tipo_evento")),
        fila(mov_pol, "MOVIMIENTO_FUERA_VIGENCIA", "MEDIA", "MOVIMIENTO", "evento_id", F.col("id_entidad"),
             F.col("fecha_evento"), F.lit(None),
             F.concat_ws(" ", F.lit("Evento"), "tipo_evento", F.lit("fuera de la vigencia de la póliza"))),
        fila(poliza_360.filter(~F.col("estado_consistente")), "ESTADO_POLIZA_INCONSISTENTE", "MEDIA", "POLIZA",
             "poliza_id", F.col("poliza_id"), F.col("fecha_fin_vigencia"), F.col("prima_emitida"),
             F.concat(F.lit("Estado fuente="), F.col("estado_fuente"), F.lit(" vs estado al corte="),
                      F.col("estado_corte"))),
        fila(poliza_360.filter(F.col("sobre_recaudo")), "RECAUDO_SUPERIOR_A_PRIMA", "BAJA", "POLIZA", "poliza_id",
             F.col("poliza_id"), F.col("ultima_fecha_pago"), F.col("recaudo_aplicado"),
             F.concat(F.lit("Recaudo aplicado supera la prima emitida en más de 5%; prima="),
                      F.col("prima_emitida").cast("string"))),
    ]
    resultado = partes[0]
    for p in partes[1:]:
        resultado = resultado.unionByName(p)
    return resultado.withColumn("_run_id", F.lit(cfg.run_id)).withColumn("_gold_ts", F.current_timestamp())


# ============================================================ reconciliación y postcondiciones
def _control(regla_id: str, descripcion: str, esperado, obtenido, ok: bool, dataset: str,
             tipo: str = "RECONCILIACION", severidad: str = "BLOQUEO") -> dict:
    return {
        "dataset": dataset, "tipo_control": tipo, "regla_id": regla_id, "dimension": "consistencia",
        "descripcion": descripcion, "severidad": severidad, "accion": None, "registros_evaluados": None,
        "registros_fallidos": 0 if ok else 1, "pct_fallo": 0.0 if ok else 100.0, "umbral_pct": 0.0,
        "valor_esperado": str(esperado), "valor_obtenido": str(obtenido),
        "resultado": "OK" if ok else ("FALLA" if severidad == "BLOQUEO" else "ADVERTENCIA"),
    }


def reconciliar_silver_gold(silver: dict[str, DataFrame], poliza_360: DataFrame, siniestralidad: DataFrame,
                            cfg: PipelineConfig) -> list[dict]:
    pol = polizas_al_corte(silver, cfg)
    ids = pol.select("poliza_id")
    s_pol = pol.agg(F.count(F.lit(1)).alias("n"), _sum(F.col("prima")).alias("prima")).collect()[0]
    s_rec = (_recaudos_validos(silver, cfg).join(ids, "poliza_id", "left_semi")
             .agg(_sum(F.when(F.col("estado_transaccion") == "Aplicado", F.col("valor"))).alias("v")).collect()[0])
    s_sin = (_siniestros_validos(silver, cfg).join(ids, "poliza_id", "left_semi")
             .agg(_sum(F.col("costo_incurrido")).alias("v")).collect()[0])
    g = poliza_360.agg(
        F.count(F.lit(1)).alias("n"), _sum(F.col("prima_emitida")).alias("prima"),
        _sum(F.col("recaudo_aplicado")).alias("rec"), _sum(F.col("costo_incurrido")).alias("costo"),
    ).collect()[0]
    m = siniestralidad.agg(
        _sum(F.col("prima_emitida")).alias("prima"), _sum(F.col("prima_recaudada")).alias("rec"),
        _sum(F.col("costo_incurrido")).alias("costo"), _sum(F.col("prima_devengada")).alias("dev"),
    ).collect()[0]
    return [
        _control("RECON_SILVER_GOLD_POLIZAS", "Pólizas Silver al corte = filas gold_poliza_360",
                 s_pol["n"], g["n"], s_pol["n"] == g["n"], "gold_poliza_360"),
        _control("RECON_SILVER_GOLD_PRIMA", "Prima emitida Silver = poliza_360 = siniestralidad_mensual",
                 s_pol["prima"], f"{g['prima']} / {m['prima']}",
                 s_pol["prima"] == g["prima"] == m["prima"], "gold"),
        _control("RECON_SILVER_GOLD_RECAUDO", "Recaudo aplicado Silver = poliza_360 = siniestralidad_mensual",
                 s_rec["v"], f"{g['rec']} / {m['rec']}", s_rec["v"] == g["rec"] == m["rec"], "gold"),
        _control("RECON_SILVER_GOLD_COSTO", "Costo incurrido Silver = poliza_360 = siniestralidad_mensual",
                 s_sin["v"], f"{g['costo']} / {m['costo']}", s_sin["v"] == g["costo"] == m["costo"], "gold"),
        _control("RECON_DEVENGADA_MENOR_EMITIDA", "Prima devengada acumulada <= prima emitida (tolerancia 0.1%)",
                 f"<= {m['prima']}", m["dev"], m["dev"] <= m["prima"] * Decimal("1.001"), "gold_siniestralidad_mensual"),
    ]


def postcondiciones(poliza_360: DataFrame, siniestralidad: DataFrame, cfg: PipelineConfig) -> list[dict]:
    mes_corte = cfg.fecha_corte.strftime("%Y-%m")
    p = poliza_360.agg(
        F.count(F.lit(1)).alias("n"), F.countDistinct("poliza_id").alias("pk"),
        F.sum(F.col("poliza_id").isNull().cast("int")).alias("nulos"),
        F.sum((~F.col("estado_corte").isin(*ESTADOS_CORTE)).cast("int")).alias("fuera_dominio"),
        F.sum((F.col("prima_emitida") <= 0).cast("int")).alias("prima_invalida"),
    ).collect()[0]
    s = siniestralidad.agg(
        F.count(F.lit(1)).alias("n"), F.countDistinct("periodo", "producto", "sucursal").alias("grano"),
        F.sum((F.col("razon_siniestralidad") < 0).cast("int")).alias("razon_neg"),
        F.max("periodo").alias("max_periodo"),
    ).collect()[0]
    post = "POSTCONDICION"
    return [
        _control("POST_P360_PK_UNICA", "Una fila por póliza en el corte", p["n"], p["pk"], p["n"] == p["pk"],
                 "gold_poliza_360", post),
        _control("POST_P360_CLAVE_NO_NULA", "poliza_id sin nulos", 0, p["nulos"], (p["nulos"] or 0) == 0,
                 "gold_poliza_360", post),
        _control("POST_P360_ESTADO_DOMINIO", "estado_corte dentro del dominio", 0, p["fuera_dominio"],
                 (p["fuera_dominio"] or 0) == 0, "gold_poliza_360", post),
        _control("POST_P360_PRIMA_POSITIVA", "Prima emitida > 0", 0, p["prima_invalida"],
                 (p["prima_invalida"] or 0) == 0, "gold_poliza_360", post),
        _control("POST_P360_NO_VACIA", "La tabla tiene registros", "> 0", p["n"], p["n"] > 0,
                 "gold_poliza_360", post),
        _control("POST_SM_GRANO_UNICO", "Una fila por periodo, producto y sucursal", s["n"], s["grano"],
                 s["n"] == s["grano"], "gold_siniestralidad_mensual", post),
        _control("POST_SM_RAZON_NO_NEGATIVA", "Razón de siniestralidad >= 0", 0, s["razon_neg"],
                 (s["razon_neg"] or 0) == 0, "gold_siniestralidad_mensual", post),
        _control("POST_SM_SIN_PERIODOS_FUTUROS", "No hay periodos posteriores al corte", f"<= {mes_corte}",
                 s["max_periodo"], (s["max_periodo"] or "") <= mes_corte, "gold_siniestralidad_mensual", post),
    ]
