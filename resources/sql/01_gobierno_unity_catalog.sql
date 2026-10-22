-- =====================================================================================
-- Gobierno con Unity Catalog: clasificacion, enmascaramiento, documentacion y permisos.
-- Variables: ${catalog} ${schema} ${volume} ${grupo_pii} ${grupo_ingenieros}
--            ${grupo_analistas} ${grupo_stewards} ${evaluador_1} ${evaluador_2}
-- Se ejecuta desde notebooks/00_configuracion_inicial (tolerando sentencias que Free Edition
-- no soporte, p. ej. grupos de cuenta inexistentes). Idempotente: CREATE OR REPLACE / SET.
-- =====================================================================================

-- ---------------------------------------------------------------- 1. Funciones de mascara
-- Solo los miembros del grupo PII ven el dato real; el resto ve el dato enmascarado.
CREATE OR REPLACE FUNCTION ${catalog}.${schema}.fn_mask_documento(valor STRING)
RETURNS STRING
COMMENT 'Mascara PII: documento visible solo para el grupo de lectores PII (ultimos 4 digitos para el resto)'
RETURN CASE WHEN is_account_group_member('${grupo_pii}') THEN valor
            WHEN valor IS NULL THEN NULL
            ELSE concat(repeat('*', greatest(length(valor) - 4, 0)), right(valor, 4)) END;

CREATE OR REPLACE FUNCTION ${catalog}.${schema}.fn_mask_correo(valor STRING)
RETURNS STRING
COMMENT 'Mascara PII: correo electronico'
RETURN CASE WHEN is_account_group_member('${grupo_pii}') THEN valor
            WHEN valor IS NULL THEN NULL
            ELSE regexp_replace(valor, '^(.).*(@.*)$', '$1***$2') END;

CREATE OR REPLACE FUNCTION ${catalog}.${schema}.fn_mask_telefono(valor STRING)
RETURNS STRING
COMMENT 'Mascara PII: telefono'
RETURN CASE WHEN is_account_group_member('${grupo_pii}') THEN valor
            WHEN valor IS NULL THEN NULL
            ELSE concat('******', right(valor, 4)) END;

CREATE OR REPLACE FUNCTION ${catalog}.${schema}.fn_mask_fecha_nacimiento(valor DATE)
RETURNS DATE
COMMENT 'Mascara PII: fecha de nacimiento generalizada al 1 de enero del anio'
RETURN CASE WHEN is_account_group_member('${grupo_pii}') THEN valor
            WHEN valor IS NULL THEN NULL
            ELSE make_date(year(valor), 1, 1) END;

-- ---------------------------------------------------------------- 2. Mascaras de columna
-- Se aplican en Silver (capa de consumo de datos de cliente). Bronze conserva el dato fiel y
-- su acceso se restringe al grupo de ingenieria (no se otorga a consumidores).
ALTER TABLE ${catalog}.${schema}.silver_clientes ALTER COLUMN numero_documento SET MASK ${catalog}.${schema}.fn_mask_documento;
ALTER TABLE ${catalog}.${schema}.silver_clientes ALTER COLUMN correo SET MASK ${catalog}.${schema}.fn_mask_correo;
ALTER TABLE ${catalog}.${schema}.silver_clientes ALTER COLUMN telefono SET MASK ${catalog}.${schema}.fn_mask_telefono;
ALTER TABLE ${catalog}.${schema}.silver_clientes ALTER COLUMN fecha_nacimiento SET MASK ${catalog}.${schema}.fn_mask_fecha_nacimiento;
ALTER TABLE ${catalog}.${schema}.silver_clientes_historia ALTER COLUMN numero_documento SET MASK ${catalog}.${schema}.fn_mask_documento;
ALTER TABLE ${catalog}.${schema}.silver_clientes_historia ALTER COLUMN correo SET MASK ${catalog}.${schema}.fn_mask_correo;
ALTER TABLE ${catalog}.${schema}.silver_clientes_historia ALTER COLUMN telefono SET MASK ${catalog}.${schema}.fn_mask_telefono;

-- ---------------------------------------------------------------- 3. Clasificacion (tags)
ALTER TABLE ${catalog}.${schema}.silver_clientes ALTER COLUMN numero_documento SET TAGS ('clasificacion' = 'PII_ALTA');
ALTER TABLE ${catalog}.${schema}.silver_clientes ALTER COLUMN correo SET TAGS ('clasificacion' = 'PII_ALTA');
ALTER TABLE ${catalog}.${schema}.silver_clientes ALTER COLUMN telefono SET TAGS ('clasificacion' = 'PII_ALTA');
ALTER TABLE ${catalog}.${schema}.silver_clientes ALTER COLUMN fecha_nacimiento SET TAGS ('clasificacion' = 'PII_MEDIA');
ALTER TABLE ${catalog}.${schema}.bronze_clientes SET TAGS ('capa' = 'bronze', 'contiene_pii' = 'si', 'acceso' = 'restringido');
ALTER TABLE ${catalog}.${schema}.bronze_polizas SET TAGS ('capa' = 'bronze', 'dominio' = 'polizas');
ALTER TABLE ${catalog}.${schema}.bronze_recaudos SET TAGS ('capa' = 'bronze', 'dominio' = 'recaudo', 'contiene_pii' = 'si');
ALTER TABLE ${catalog}.${schema}.bronze_siniestros SET TAGS ('capa' = 'bronze', 'dominio' = 'siniestros');
ALTER TABLE ${catalog}.${schema}.bronze_movimientos SET TAGS ('capa' = 'bronze', 'dominio' = 'eventos');
ALTER TABLE ${catalog}.${schema}.silver_clientes SET TAGS ('capa' = 'silver', 'dominio' = 'clientes', 'contiene_pii' = 'si', 'owner_negocio' = 'Dominio Clientes');
ALTER TABLE ${catalog}.${schema}.silver_polizas SET TAGS ('capa' = 'silver', 'dominio' = 'polizas', 'owner_negocio' = 'Dominio Polizas');
ALTER TABLE ${catalog}.${schema}.silver_recaudos SET TAGS ('capa' = 'silver', 'dominio' = 'recaudo', 'owner_negocio' = 'Dominio Financiero');
ALTER TABLE ${catalog}.${schema}.silver_siniestros SET TAGS ('capa' = 'silver', 'dominio' = 'siniestros', 'owner_negocio' = 'Dominio Indemnizaciones');
ALTER TABLE ${catalog}.${schema}.silver_movimientos SET TAGS ('capa' = 'silver', 'dominio' = 'eventos');
ALTER TABLE ${catalog}.${schema}.silver_cuarentena SET TAGS ('capa' = 'silver', 'tipo' = 'cuarentena', 'contiene_pii' = 'si', 'acceso' = 'restringido');
ALTER TABLE ${catalog}.${schema}.gold_poliza_360 SET TAGS ('capa' = 'gold', 'producto_datos' = 'si', 'certificable' = 'si', 'owner_negocio' = 'Dominio Polizas');
ALTER TABLE ${catalog}.${schema}.gold_siniestralidad_mensual SET TAGS ('capa' = 'gold', 'producto_datos' = 'si', 'certificable' = 'si', 'owner_negocio' = 'Actuaria');
ALTER TABLE ${catalog}.${schema}.gold_calidad_pipeline SET TAGS ('capa' = 'gold', 'producto_datos' = 'si', 'tipo' = 'observabilidad');
ALTER TABLE ${catalog}.${schema}.gold_discrepancias SET TAGS ('capa' = 'gold', 'producto_datos' = 'si', 'owner_negocio' = 'Control Financiero');

-- ---------------------------------------------------------------- 4. Documentacion de columnas clave
COMMENT ON COLUMN ${catalog}.${schema}.gold_poliza_360.estado_corte IS 'Estado calculado a la fecha de corte: VIGENTE, PROXIMA_A_VENCER (fin <= corte + 30 dias), VENCIDA, RENOVADA, CANCELADA, NO_INICIADA';
COMMENT ON COLUMN ${catalog}.${schema}.gold_poliza_360.recaudo_aplicado IS 'Suma de recaudos en estado Aplicado con fecha_pago <= fecha_corte (COP)';
COMMENT ON COLUMN ${catalog}.${schema}.gold_poliza_360.costo_incurrido IS 'Valor pagado + reserva de siniestros avisados hasta la fecha de corte (COP)';
COMMENT ON COLUMN ${catalog}.${schema}.gold_poliza_360.ultima_actividad_fecha IS 'Fecha mas reciente entre actualizacion de poliza, pago, siniestro y evento (<= corte)';
COMMENT ON COLUMN ${catalog}.${schema}.gold_siniestralidad_mensual.prima_devengada IS 'Prima devengada pro rata temporis por dias de vigencia en el mes (COP)';
COMMENT ON COLUMN ${catalog}.${schema}.gold_siniestralidad_mensual.razon_siniestralidad IS 'costo_incurrido / prima_devengada del periodo (null si no hay prima devengada)';
COMMENT ON COLUMN ${catalog}.${schema}.gold_siniestralidad_mensual.polizas_expuestas IS 'Polizas con al menos un dia de vigencia en el mes';

-- ---------------------------------------------------------------- 5. Permisos por rol (minimo privilegio)
-- Ingenieria de datos: administra los objetos del schema (en corporativo: service principal del job).
GRANT USE CATALOG ON CATALOG ${catalog} TO `${grupo_ingenieros}`;
GRANT USE SCHEMA, CREATE TABLE, CREATE FUNCTION, MODIFY, SELECT ON SCHEMA ${catalog}.${schema} TO `${grupo_ingenieros}`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME ${catalog}.${schema}.${volume} TO `${grupo_ingenieros}`;
-- Data stewards: calidad, cuarentena y Silver (sin Bronze).
GRANT USE CATALOG ON CATALOG ${catalog} TO `${grupo_stewards}`;
GRANT USE SCHEMA ON SCHEMA ${catalog}.${schema} TO `${grupo_stewards}`;
GRANT SELECT ON TABLE ${catalog}.${schema}.silver_cuarentena TO `${grupo_stewards}`;
GRANT SELECT ON TABLE ${catalog}.${schema}.gold_calidad_pipeline TO `${grupo_stewards}`;
GRANT SELECT ON TABLE ${catalog}.${schema}.silver_polizas TO `${grupo_stewards}`;
GRANT SELECT ON TABLE ${catalog}.${schema}.silver_clientes TO `${grupo_stewards}`;
-- Analistas / BI / IA: solo productos Gold y vistas certificadas.
GRANT USE CATALOG ON CATALOG ${catalog} TO `${grupo_analistas}`;
GRANT USE SCHEMA ON SCHEMA ${catalog}.${schema} TO `${grupo_analistas}`;
GRANT SELECT ON TABLE ${catalog}.${schema}.gold_poliza_360 TO `${grupo_analistas}`;
GRANT SELECT ON TABLE ${catalog}.${schema}.gold_siniestralidad_mensual TO `${grupo_analistas}`;
GRANT SELECT ON TABLE ${catalog}.${schema}.gold_discrepancias TO `${grupo_analistas}`;
GRANT SELECT ON VIEW ${catalog}.${schema}.vw_gold_poliza_360_certificada TO `${grupo_analistas}`;
GRANT SELECT ON VIEW ${catalog}.${schema}.vw_gold_siniestralidad_certificada TO `${grupo_analistas}`;
-- Evaluadores de la prueba: rol revisor de solo lectura sobre el schema y el volumen.
GRANT USE CATALOG ON CATALOG ${catalog} TO `${evaluador_1}`;
GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA ${catalog}.${schema} TO `${evaluador_1}`;
GRANT READ VOLUME ON VOLUME ${catalog}.${schema}.${volume} TO `${evaluador_1}`;
GRANT USE CATALOG ON CATALOG ${catalog} TO `${evaluador_2}`;
GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA ${catalog}.${schema} TO `${evaluador_2}`;
GRANT READ VOLUME ON VOLUME ${catalog}.${schema}.${volume} TO `${evaluador_2}`;
