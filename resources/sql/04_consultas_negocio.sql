-- =====================================================================================
-- Consultas que responden las preguntas de negocio (seccion 4.1 de la prueba).
-- Se ejecutan sobre workspace.default; cambie el catalogo/schema si aplica.
-- =====================================================================================

-- P1. Polizas vigentes, vencidas, canceladas o proximas a vencer en la fecha de corte
SELECT fecha_corte, estado_corte, count(*) AS polizas, sum(prima_emitida) AS prima_emitida
FROM workspace.default.vw_gold_poliza_360_certificada
GROUP BY fecha_corte, estado_corte
ORDER BY polizas DESC;

-- P2. Prima emitida, recaudada y pendiente por producto, sucursal y mes
SELECT periodo, producto, sucursal, prima_emitida, prima_recaudada, recaudo_pendiente,
       prima_pendiente_acumulada
FROM workspace.default.vw_gold_siniestralidad_certificada
ORDER BY periodo DESC, producto, sucursal;

-- P3. Costo incurrido y razon de siniestralidad mensual (total compania)
SELECT periodo,
       sum(prima_devengada) AS prima_devengada,
       sum(costo_incurrido) AS costo_incurrido,
       round(sum(costo_incurrido) / nullif(sum(prima_devengada), 0), 4) AS razon_siniestralidad
FROM workspace.default.vw_gold_siniestralidad_certificada
GROUP BY periodo
ORDER BY periodo DESC;

-- P4. Pagos sin correspondencia, siniestros sin poliza valida y movimientos fuera de vigencia
SELECT tipo_discrepancia, severidad, count(*) AS casos, sum(valor) AS valor_total
FROM workspace.default.gold_discrepancias
WHERE fecha_corte = (SELECT max(fecha_corte) FROM workspace.default.ctl_publicaciones WHERE estado = 'PUBLICADO')
GROUP BY tipo_discrepancia, severidad
ORDER BY severidad, casos DESC;

-- P5. Reglas de calidad incumplidas y registros en cuarentena
SELECT capa, dataset, regla_id, severidad, resultado, registros_fallidos, pct_fallo, umbral_pct
FROM workspace.default.gold_calidad_pipeline
WHERE run_id = (SELECT run_id FROM workspace.default.vw_obs_ultima_ejecucion_exitosa LIMIT 1)
  AND (registros_fallidos > 0 OR resultado <> 'OK')
ORDER BY capa, dataset, regla_id;

SELECT dataset, regla_id, severidad, estado_tratamiento, count(*) AS registros
FROM workspace.default.silver_cuarentena
GROUP BY dataset, regla_id, severidad, estado_tratamiento
ORDER BY dataset, registros DESC;

-- P6. Ultima ejecucion exitosa y datos procesados
SELECT * FROM workspace.default.vw_obs_ultima_ejecucion_exitosa;
