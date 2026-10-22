-- =====================================================================================
-- Vistas de consumo certificadas: exponen SOLO el ultimo corte publicado (validado) en
-- ctl_publicaciones. Es la fuente recomendada para BI, analitica e IA generativa.
-- Variables: ${catalog} ${schema}
-- =====================================================================================

CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_gold_poliza_360_certificada
COMMENT 'Ultimo corte certificado de gold_poliza_360 (sin datos sensibles)'
AS
SELECT p.fecha_corte, p.poliza_id, p.cliente_id, p.cliente_existe, p.cliente_segmento, p.cliente_ciudad,
       p.producto, p.sucursal, p.canal, p.fecha_inicio_vigencia, p.fecha_fin_vigencia, p.fecha_fin_efectiva,
       p.estado_fuente, p.estado_corte, p.estado_consistente, p.dias_para_vencer, p.prima_emitida,
       p.recaudo_aplicado, p.recaudo_pendiente, p.recaudo_reversado, p.saldo_por_recaudar, p.pct_recaudo,
       p.num_recaudos, p.num_siniestros, p.num_siniestros_abiertos, p.costo_incurrido,
       p.ultima_actividad_fecha, p.ultima_actividad_origen, p.ultimo_evento, p.advertencias_calidad,
       pub.run_id AS run_id_publicacion, pub.ts_publicacion
FROM ${catalog}.${schema}.gold_poliza_360 p
JOIN (SELECT fecha_corte, run_id, ts_publicacion
      FROM ${catalog}.${schema}.ctl_publicaciones
      WHERE estado = 'PUBLICADO'
      ORDER BY fecha_corte DESC LIMIT 1) pub
  ON p.fecha_corte = pub.fecha_corte;

CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_gold_siniestralidad_certificada
COMMENT 'Ultimo corte certificado de gold_siniestralidad_mensual'
AS
SELECT s.fecha_corte, s.periodo, s.producto, s.sucursal, s.polizas_expuestas, s.prima_devengada,
       s.polizas_emitidas, s.prima_emitida, s.prima_recaudada, s.recaudo_pendiente,
       s.prima_pendiente_acumulada, s.num_siniestros, s.valor_pagado, s.valor_reserva,
       s.costo_incurrido, s.razon_siniestralidad, pub.run_id AS run_id_publicacion
FROM ${catalog}.${schema}.gold_siniestralidad_mensual s
JOIN (SELECT fecha_corte, run_id
      FROM ${catalog}.${schema}.ctl_publicaciones
      WHERE estado = 'PUBLICADO'
      ORDER BY fecha_corte DESC LIMIT 1) pub
  ON s.fecha_corte = pub.fecha_corte;

CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_clientes_protegida
COMMENT 'Clientes con PII enmascarada por vista (alternativa portable a las mascaras de columna)'
AS
SELECT cliente_id, tipo_documento,
       ${catalog}.${schema}.fn_mask_documento(numero_documento) AS numero_documento,
       ${catalog}.${schema}.fn_mask_fecha_nacimiento(fecha_nacimiento) AS fecha_nacimiento,
       ciudad, segmento,
       ${catalog}.${schema}.fn_mask_correo(correo) AS correo,
       ${catalog}.${schema}.fn_mask_telefono(telefono) AS telefono,
       fecha_actualizacion, _dq_advertencias
FROM ${catalog}.${schema}.silver_clientes;
