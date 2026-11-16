# Modelo de datos, productos Gold y calidad

## 1. Modelo por capas

| Capa | Tabla | Grano | Clave | Escritura |
|---|---|---|---|---|
| Bronze | `bronze_<dataset>` (5) | Fila física recibida por archivo | `_record_id` | `replaceWhere _file_hash` |
| Silver | `silver_clientes` | Cliente vigente | `cliente_id` | MERGE |
| Silver | `silver_clientes_historia` | Versión de cliente (SCD2) | `cliente_id, version` | Overwrite determinístico |
| Silver | `silver_polizas` | Póliza (última versión) | `poliza_id` | MERGE |
| Silver | `silver_recaudos` | Transacción de recaudo | `recaudo_id` | MERGE |
| Silver | `silver_siniestros` | Siniestro (última actualización) | `siniestro_id` | MERGE |
| Silver | `silver_movimientos` | Evento | `evento_id` | MERGE |
| Silver | `silver_cuarentena` | Registro × regla incumplida | `dataset, _record_id, regla_id` | MERGE |
| Gold | `gold_poliza_360` | Póliza × fecha de corte | `fecha_corte, poliza_id` | `replaceWhere fecha_corte` |
| Gold | `gold_siniestralidad_mensual` | Mes × producto × sucursal × corte | `fecha_corte, periodo, producto, sucursal` | `replaceWhere fecha_corte` |
| Gold | `gold_discrepancias` | Hallazgo × corte | `fecha_corte, tipo_discrepancia, id_registro` | `replaceWhere fecha_corte` |
| Gold | `gold_calidad_pipeline` | Control × ejecución | `run_id, capa, dataset, tipo_control, regla_id` | MERGE |

**Dimensiones conformadas.** `cliente` (segmento, ciudad, tipo de documento), `producto`, `sucursal`, `canal`, `periodo` (mes) y `fecha_corte` se usan con los mismos valores en todos los productos Gold. Los productos o sucursales nulos se publican como `NO INFORMADO` / `NO INFORMADA`, con la advertencia de calidad trazable. **Hechos:** prima emitida y devengada, recaudos, siniestros (pagado, reserva, costo incurrido) y eventos.

**Historización.**
- Bronze guarda todo lo recibido.
- Silver guarda el estado vigente (SCD1). Las versiones anteriores quedan en Bronze.
- `silver_clientes_historia` es SCD2 (`valido_desde`, `valido_hasta`, `es_actual`), y las versiones consecutivas idénticas se colapsan.
- Gold es una **foto por corte**: los cortes anteriores no se sobrescriben, lo que permite comparar meses y auditar lo publicado. Reprocesar un corte reemplaza solo esa foto.

## 2. gold_poliza_360
Una fila por póliza al corte. Incluye las pólizas aceptadas en Silver con `fecha_inicio_vigencia <= fecha_corte`.

| Grupo | Columnas | Regla |
|---|---|---|
| Identidad | `poliza_id`, `cliente_id`, `cliente_existe`, `cliente_segmento`, `cliente_ciudad`, `producto`, `sucursal`, `canal` | Sin PII |
| Vigencia | `fecha_inicio_vigencia`, `fecha_fin_vigencia`, `fecha_fin_efectiva`, `dias_vigencia`, `dias_para_vencer` | `fecha_fin_efectiva` = `fecha_actualizacion` para canceladas, si cae dentro de la vigencia |
| Estado | `estado_fuente`, `estado_corte`, `estado_consistente` | Ver la regla de estado al corte, debajo de esta tabla |
| Prima y recaudo | `prima_emitida`, `recaudo_aplicado`, `recaudo_pendiente`, `recaudo_reversado`, `recaudo_rechazado`, `saldo_por_recaudar`, `pct_recaudo`, `sobre_recaudo`, `num_recaudos`, `num_recaudos_fuera_vigencia`, `ultima_fecha_pago` | Recaudos con `fecha_pago <= corte` y póliza válida; `saldo = prima − aplicado` |
| Siniestros | `num_siniestros`, `num_siniestros_abiertos`, `siniestros_valor_pagado`, `siniestros_valor_reserva`, `costo_incurrido`, `num_siniestros_fuera_vigencia` | Siniestros con `fecha_aviso <= corte` (lo conocido al corte); costo = pagado + reserva |
| Actividad | `num_eventos`, `ultimo_evento`, `ultima_fecha_evento`, `ultima_actividad_fecha`, `ultima_actividad_origen` | Máximo entre actualización de póliza, pago, siniestro y evento (todas ≤ corte); el evento más reciente se ordena por (`fecha_evento`, `secuencia`, `fecha_ingesta`) |
| Trazabilidad | `advertencias_calidad`, `_run_id`, `_gold_ts` | |

**Regla de estado al corte** (`estado_corte`). Se evalúa en este orden y gana la primera condición que se cumpla:
1. `CANCELADA`: la fuente reporta la póliza como cancelada.
2. Fin de vigencia anterior al corte: `RENOVADA` si la fuente la reporta como renovada; si no, `VENCIDA`.
3. `NO_INICIADA`: la vigencia empieza después del corte.
4. `PROXIMA_A_VENCER`: la vigencia termina dentro de los 30 días siguientes al corte (parámetro `dias_proxima_a_vencer`).
5. `VIGENTE`: cualquier otro caso.

## 3. gold_siniestralidad_mensual
Una fila por `periodo` (`yyyy-MM`) × `producto` × `sucursal`, desde el primer mes con datos hasta el mes del corte.

| Métrica | Definición |
|---|---|
| `polizas_expuestas` | Pólizas con al menos un día de vigencia efectiva en el mes |
| `dias_exposicion` | Días-póliza expuestos en el mes |
| `prima_devengada` | `prima × días de vigencia en el mes ∩ [inicio, fin_efectiva) ∩ (≤ corte) / días totales de vigencia` (pro rata temporis) |
| `polizas_emitidas`, `prima_emitida` | Pólizas y prima por mes de inicio de vigencia |
| `prima_recaudada` | Recaudos *Aplicado* por mes de pago |
| `recaudo_pendiente` | Recaudos *Pendiente* por mes de pago |
| `prima_pendiente_acumulada` | Emitida acumulada − recaudada acumulada (por producto y sucursal) |
| `num_siniestros`, `valor_pagado`, `valor_reserva`, `costo_incurrido` | Siniestros por mes de **ocurrencia** (año/mes de accidente), avisados ≤ corte |
| `razon_siniestralidad` | `costo_incurrido / prima_devengada` (null si no hay prima devengada) |

## 4. gold_calidad_pipeline
Una fila por ejecución × capa × dataset × control. `tipo_control` ∈ {`REGLA`, `CONTRATO`, `RECONCILIACION`, `POSTCONDICION`, `FRESCURA`}, con `severidad`, `registros_evaluados`, `registros_fallidos`, `pct_fallo`, `umbral_pct`, `valor_esperado`, `valor_obtenido` y `resultado` ∈ {`OK`, `ADVERTENCIA`, `FALLA`, `INFO`}.

## 5. gold_discrepancias (adicional)
`POLIZA_SIN_CLIENTE`, `RECAUDO_SIN_POLIZA_VALIDA`, `SINIESTRO_SIN_POLIZA_VALIDA`, `RECAUDO_FUERA_VIGENCIA`, `SINIESTRO_FUERA_VIGENCIA`, `MOVIMIENTO_ENTIDAD_INEXISTENTE`, `MOVIMIENTO_FUERA_VIGENCIA`, `ESTADO_POLIZA_INCONSISTENTE` y `RECAUDO_SUPERIOR_A_PRIMA`, con severidad, valor y detalle. Responde la pregunta: *¿qué pólizas presentan pagos sin correspondencia, siniestros sin póliza válida o movimientos fuera de vigencia?*

## 6. Calidad de datos

### Semántica del motor
- Cada regla es una expresión SQL que debe ser **verdadera**. Un resultado `null` cuenta como incumplimiento.
- La **acción** se aplica a nivel de registro:
  - `CUARENTENA`: el registro sale de Silver.
  - `MARCAR`: el registro continúa a Silver, con la regla anotada en `_dq_advertencias`.
- El **umbral** se evalúa a nivel de dataset. Si el % de incumplimiento lo supera:

| Severidad | Resultado | Comportamiento del pipeline |
|---|---|---|
| BLOQUEO | FALLA | Se registran las evidencias, la tarea falla y Gold no se publica |
| ADVERTENCIA | ADVERTENCIA | Alerta; el pipeline continúa |
| INFORMATIVA | INFO | Solo métrica |

### Catálogo de reglas (Silver)
Comunes a los 5 datasets (prefijo `CLI`/`POL`/`REC`/`SIN`/`MOV`):

| Regla | Dimensión | Severidad | Acción | Umbral |
|---|---|---|---|---|
| `*_ESTRUCTURA_FILA` (fila no malformada) | validez | BLOQUEO | CUARENTENA | 1% |
| `*_PK_NO_NULA` | completitud | BLOQUEO | CUARENTENA | 1% |
| `*_TIPOS_VALIDOS` (fechas/montos convertibles) | validez | BLOQUEO | CUARENTENA | 2% |
| `*_DUP_EXACTO` | unicidad | ADVERTENCIA | CUARENTENA | 5% |
| `*_DUP_NEGOCIO` | unicidad | ADVERTENCIA | CUARENTENA | 5% |
| `*_EMPATE_VERSION` | consistencia | INFORMATIVA | MARCAR | – |
| `*_<COLUMNA>_DOMINIO` (valores permitidos) | validez | ADVERTENCIA | CUARENTENA | 2% |

Específicas:

| Regla | Dimensión | Severidad | Acción | Umbral |
|---|---|---|---|---|
| `CLI_DOCUMENTO_REQUERIDO` | completitud | ADVERTENCIA | MARCAR | 5% |
| `CLI_FECHA_ACT_REQUERIDA` | completitud | ADVERTENCIA | CUARENTENA | 2% |
| `CLI_NACIMIENTO_VALIDO` | validez | ADVERTENCIA | CUARENTENA | 2% |
| `CLI_CORREO_FORMATO` | validez | INFORMATIVA | MARCAR | – |
| `CLI_DOCUMENTO_UNICO` | unicidad | ADVERTENCIA | MARCAR | 2% |
| `POL_CLIENTE_REQUERIDO` | completitud | BLOQUEO | CUARENTENA | 1% |
| `POL_PRODUCTO_REQUERIDO`, `POL_SUCURSAL_REQUERIDA` | completitud | ADVERTENCIA | MARCAR | 2% |
| `POL_VIGENCIA_REQUERIDA` | completitud | BLOQUEO | CUARENTENA | 1% |
| `POL_PRIMA_POSITIVA` | validez | BLOQUEO | CUARENTENA | 2% |
| `POL_VIGENCIA_COHERENTE` (fin ≥ inicio) | consistencia | BLOQUEO | CUARENTENA | 2% |
| `POL_DURACION_RAZONABLE` (≤ 5 años) | validez | ADVERTENCIA | MARCAR | 2% |
| `POL_CLIENTE_EXISTE` | integridad referencial | ADVERTENCIA | MARCAR | 2% |
| `POL_MONEDA_COP` | validez | INFORMATIVA | MARCAR | – |
| `REC_POLIZA_REQUERIDA` | completitud | BLOQUEO | CUARENTENA | 1% |
| `REC_FECHA_PAGO_REQUERIDA` | completitud | ADVERTENCIA | CUARENTENA | 2% |
| `REC_VALOR_POSITIVO` | validez | BLOQUEO | CUARENTENA | 2% |
| `REC_POLIZA_EXISTE` | integridad referencial | ADVERTENCIA | MARCAR | 2% |
| `REC_PAGO_EN_VIGENCIA` | consistencia | ADVERTENCIA | MARCAR | 60% |
| `REC_INGESTA_POSTERIOR_PAGO` | consistencia | ADVERTENCIA | MARCAR | 2% |
| `REC_OPORTUNIDAD` (≤ 30 días) | oportunidad | INFORMATIVA | MARCAR | – |
| `SIN_POLIZA_REQUERIDA` | completitud | BLOQUEO | CUARENTENA | 1% |
| `SIN_ESTADO_REQUERIDO` | completitud | ADVERTENCIA | MARCAR | 2% |
| `SIN_PAGADO_NO_NEGATIVO`, `SIN_RESERVA_NO_NEGATIVA` | validez | BLOQUEO | CUARENTENA | 2% |
| `SIN_AVISO_POSTERIOR_OCURRENCIA` | consistencia | ADVERTENCIA | CUARENTENA | 2% |
| `SIN_POLIZA_EXISTE` | integridad referencial | ADVERTENCIA | MARCAR | 2% |
| `SIN_OCURRENCIA_EN_VIGENCIA` | consistencia | ADVERTENCIA | MARCAR | 90% |
| `SIN_OPORTUNIDAD_AVISO` (≤ 30 días) | oportunidad | INFORMATIVA | MARCAR | – |
| `MOV_ID_ENTIDAD_REQUERIDO` | completitud | BLOQUEO | CUARENTENA | 1% |
| `MOV_FECHA_EVENTO_REQUERIDA` | completitud | ADVERTENCIA | CUARENTENA | 2% |
| `MOV_PAYLOAD_VALIDO` | validez | ADVERTENCIA | CUARENTENA | 2% |
| `MOV_EVENTO_COHERENTE` (tipo válido para la entidad) | consistencia | ADVERTENCIA | CUARENTENA | 2% |
| `MOV_ENTIDAD_EXISTE` | integridad referencial | ADVERTENCIA | MARCAR | 2% |
| `MOV_INGESTA_POSTERIOR_EVENTO` | consistencia | ADVERTENCIA | MARCAR | 2% |
| `MOV_LLEGADA_TARDIA` (≤ 30 días) | oportunidad | INFORMATIVA | MARCAR | – |

**Controles de dataset:**

| Control | Momento | Severidad |
|---|---|---|
| `CONTRATO_ESQUEMA_*` (columnas obligatorias) | Bronze | BLOQUEO |
| `EVOLUCION_ESQUEMA_*` (columnas nuevas) | Bronze | ADVERTENCIA |
| `FRESCURA_*` (último dato de negocio a ≤ 35 días del corte) | Gold | ADVERTENCIA |

**Errores vs. excepciones válidas.** No se "corrige" el dato:
- Los **errores** (montos negativos, fechas imposibles, duplicados) van a cuarentena con su causa.
- Las **excepciones válidas o a investigar** (huérfanos, pagos fuera de vigencia, llegadas tardías, documentos compartidos) continúan marcadas y se publican como discrepancias.

### Cuarentena
`silver_cuarentena` guarda una fila por registro y regla incumplida, con estos campos:
- `registro_original` (JSON con los valores **tal como llegaron**);
- `regla_id`, `dimension`, `severidad`, `descripcion_regla`;
- `_source_file`, `_file_hash`, `_record_id`;
- `run_id` de la primera detección, `run_id_ultima_deteccion`, `pipeline`, `fecha_corte`;
- `estado_tratamiento`: `PENDIENTE` o `RESUELTO`. Pasa a `RESUELTO` cuando el registro deja de llegar con el error, por ejemplo porque fue corregido en la fuente.

### Reconciliación

| Tramo | Controles |
|---|---|
| Bronze → Silver | Conteo (Bronze = Silver + cuarentena + versiones reemplazadas), claves de negocio, totales monetarios por columna, persistencia (aceptados = filas en la tabla) |
| Silver → Gold | Pólizas, prima emitida, recaudo aplicado y costo incurrido, comparados contra `poliza_360` y `siniestralidad_mensual`; devengada ≤ emitida |
| Postcondiciones Gold | PK única, clave no nula, dominio de estado, prima > 0, tabla no vacía, grano único, razón ≥ 0, sin periodos futuros |

Todas las reconciliaciones son de severidad BLOQUEO: una diferencia indica un defecto del pipeline y **impide la publicación**.
