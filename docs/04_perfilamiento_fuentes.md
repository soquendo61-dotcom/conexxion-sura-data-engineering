# Perfilamiento de fuentes, hallazgos y riesgos

Archivos: CSV separados por `;`, UTF-8 con BOM, encabezado en la primera fila, todos los valores como texto.
El volumen real es mucho menor que el "orientativo" del enunciado; la solución está diseñada para escalar
(Spark distribuido, sin `collect` de datasets, clustering, escritura incremental).

| Dataset | Filas | Columnas | Volumen orientativo |
|---|---|---|---|
| clientes | 502 | 10 | 100 mil |
| polizas | 1.502 | 12 | 500 mil |
| recaudos | 5.002 | 9 | 2 millones |
| siniestros | 2.502 | 11 | 800 mil |
| movimientos | 8.002 | 9 | 3 millones |

## Hallazgos por situación intencional

| Situación | Evidencia encontrada | Tratamiento |
|---|---|---|
| Duplicados exactos | 1 fila repetida por archivo (`CLI000021`, `POL0000101`, `REC00000201`, `SIN00000301`, `EVT000000501`) | Regla `*_DUP_EXACTO` → cuarentena (se conserva la primera) |
| Duplicados por clave de negocio | Mismo hecho con otro ID técnico: `CLI999901`≡`CLI000076`, `POL9999998`≡`POL0000251`, `REC99999991`≡`REC00000701` (misma referencia bancaria), `SIN99999991`≡`SIN00000901`, `EVT999999991`≡`EVT000001201` | Huella de contenido sin ID → `*_DUP_NEGOCIO` → cuarentena; se conserva el ID original |
| Fechas con formatos distintos | `yyyy-MM-dd` (~95%), `yyyy/MM/dd`, `dd/MM/yyyy` en todas las columnas de fecha; 280 valores `dd/MM` ambiguos | Parseo multiformato; supuesto día/mes; valor no convertible → `*_TIPOS_VALIDOS` |
| Nulos en obligatorios | producto (5), estado de siniestro (5), fecha de pago (6), fecha de evento (8), número de documento (6) | Según impacto: cuarentena (fechas de pago/evento) o marca + "NO INFORMADO" (producto, estado, documento) |
| Cambios de esquema | Nombres distintos al enunciado (`fecha_inicio_vigencia` vs `inicio_vigencia`, `valor_pagado` vs `pagado`); un único archivo por dataset | Alias en el contrato; evolución de esquema en Bronze (`mergeSchema`) y reporte; demo con archivo nuevo en `notebooks/09` |
| Movimientos tardíos | 3.825 recaudos (76%) con ingesta > 30 días después del pago (máx. 1.226 días); 2.471 eventos (31%) > 30 días; 3 eventos ingeridos antes de ocurrir | Reglas de oportunidad (informativas) y consistencia; backfill por periodo |
| Pólizas sin cliente / siniestros sin póliza | 8 pólizas con `CLI_ORF*`, 12 recaudos `POL_ORF*`, 10 siniestros `POL_ORF_S*`, 20 eventos sobre entidades inexistentes | Se conservan marcados (`_fk_*_ok = false`) y se publican en `gold_discrepancias` |
| Montos negativos o inconsistentes | 5 primas, 8 recaudos "Aplicado" y 6 valores pagados negativos; 748 siniestros con pagado > reserva | Negativos → cuarentena (BLOQUEO con umbral 2%); pagado > reserva es posible (reserva remanente), no se trata como error |
| Fechas incoherentes | 5 pólizas con fin (2022-01-01) < inicio; 5 siniestros avisados antes de ocurrir | Cuarentena por consistencia |
| Registros que llegan nuevamente | Reenvíos con otro ID (ver duplicados de negocio); reejecuciones del mismo archivo | Hash de archivo (no se reingiere) + MERGE por clave |
| Actualizaciones con la misma marca temporal | 12 entidades con varios eventos en la misma fecha | Desempate determinístico (`secuencia`, `fecha_ingesta`, archivo, hash) + regla informativa `*_EMPATE_VERSION` |

## Otros hallazgos
- 2 documentos compartidos por clientes distintos (`NIT 800002033`: CLI000107/108; `CC 10006068`: CLI000164/165) → regla `CLI_DOCUMENTO_UNICO` (advertencia).
- Estado reportado vs. fechas: 1 póliza "Vencida" con fin posterior al corte y 1 "Renovada" vigente → `ESTADO_POLIZA_INCONSISTENTE`.
- 49% de los pagos y 79% de los siniestros caen fuera de la vigencia de su póliza → discrepancias de negocio (umbral de advertencia 60% / 90%). Proporción típica de datos sintéticos aleatorios; en producción sería un indicador de fraude o de error de asignación.
- 462 pólizas con recaudo aplicado > 105% de la prima → `RECAUDO_SUPERIOR_A_PRIMA` (baja).
- Payload JSON homogéneo (`estado_origen`, `estado_destino`, `usuario_proceso`) → se parsea en Silver con esquema explícito.
- Combinaciones entidad/tipo de evento consistentes (14 tipos) → regla `MOV_EVENTO_COHERENTE`.

## Resultado del procesamiento (corte 2026-08-31, validado localmente con los datos reales)

| Dataset | Bronze | Silver | Cuarentena | Motivo de cuarentena |
|---|---|---|---|---|
| clientes | 502 | 500 | 2 | 1 duplicado exacto, 1 duplicado de negocio |
| polizas | 1.502 | 1.490 | 12 | 1 exacto, 1 negocio, 5 primas ≤ 0, 5 vigencias incoherentes |
| recaudos | 5.002 | 4.986 | 16 | 1 exacto, 1 negocio, 6 sin fecha de pago, 8 valores ≤ 0 |
| siniestros | 2.502 | 2.489 | 13 | 1 exacto, 1 negocio, 6 pagados negativos, 5 avisos previos a la ocurrencia |
| movimientos | 8.002 | 7.992 | 10 | 1 exacto, 1 negocio, 8 sin fecha de evento |

Las reconciliaciones Bronze = Silver + cuarentena + reemplazados (conteo, claves y montos) y Silver → Gold
(1.490 pólizas; prima 11.165.022.382,05; recaudo aplicado 7.211.692.057,25; costo incurrido 79.522.530.398,65)
cuadran exactamente. Ninguna regla BLOQUEO supera su umbral.
