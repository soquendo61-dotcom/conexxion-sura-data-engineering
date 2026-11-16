# Evidencias de ejecución (Databricks Free Edition)

## Última ejecución exitosa

| Dato | Valor |
|---|---|
| Workflow | `[dev] pipeline_polizas_siniestros` (5 tareas: bronze → silver → gold → validar_publicar → gobierno_y_vistas) |
| Programación | `0 0 6 5 * ?` · America/Bogota (día 5 de cada mes, 06:00 a. m.) · notificación de fallo por correo |
| Última ejecución exitosa | Run ID `847526805095966` · inicio **22-oct-2026 10:32 p. m. (COT)** · duración 9 min 2 s · *Succeeded* |
| Ejecución exitosa previa | Run ID `121540678301467` · 22-oct-2026 10:27 p. m. (COT) · 7 min 50 s · *Succeeded* |
| Corte publicado por el job | `2026-09-30` (fecha_corte vacía = cierre del mes anterior). El corte `2026-08-31` se publicó con `notebooks/06_pipeline_completo` |

## Objetos en Unity Catalog (`workspace.default`)
- 30 tablas/vistas: `bronze_*` (5), `silver_*` (7, incluye `silver_cuarentena` y `silver_clientes_historia`),
  `gold_*` (4), `ctl_*` (5), `vw_gold_*_certificada` (2), `vw_clientes_protegida`, `vw_obs_*` (6).
- 4 funciones de enmascaramiento `fn_mask_*` aplicadas como máscaras de columna en `silver_clientes`.
- Volumen `volume1` con los archivos fuente en `raw/`.
- Permisos de solo lectura (`USE SCHEMA`, `SELECT`, `EXECUTE`) para `yesidmontoya@sura.com.co` y `fjcarvajal@sura.com.co`.
- Máscara verificada: documento `****0111`, correo `c***@ejemplo.com`, teléfono `******4578`, nacimiento generalizado a 1 de enero.

## Resultados del corte 2026-08-31

| Dataset | Bronze | Silver | Cuarentena |
|---|---|---|---|
| clientes | 502 | 500 | 2 |
| polizas | 1.502 | 1.490 | 12 |
| recaudos | 5.002 | 4.986 | 16 |
| siniestros | 2.502 | 2.489 | 13 |
| movimientos | 8.002 | 7.992 | 10 |

- Reglas BLOQUEO: todas por debajo del umbral. Reconciliaciones Bronze→Silver y Silver→Gold y postcondiciones: OK.
- `gold_poliza_360`: 1.490 pólizas (236 vigentes, 39 próximas a vencer, 669 vencidas, 262 renovadas, 284 canceladas).
- `gold_siniestralidad_mensual`: 1.643 filas (44 meses × producto × sucursal).
- `gold_discrepancias`: 1.959 siniestros fuera de vigencia, 2.429 recaudos fuera de vigencia, 2.312 movimientos
  fuera de vigencia, 53 recaudos y 29 siniestros sin póliza válida, 8 pólizas sin cliente, 53 movimientos sobre
  entidades inexistentes, 462 recaudos superiores a la prima, 2 estados inconsistentes.

## Escenarios operativos (`notebooks/09_demo_cuarentena_reproceso_errores`)

| Escenario | Evidencia |
|---|---|
| A. Nueva entrega con cambio de esquema | `polizas_2026_09_novedades.csv` ingerido con columnas nuevas `canal_detalle, valor_prima` (alias resuelto a `prima`, alerta `EVOLUCION_ESQUEMA_POLIZAS`); `POL0002002` en cuarentena por `POL_PRIMA_POSITIVA`; `POL0000001` actualizada a Cancelada; `POL0002001` nueva con fecha `dd/MM/yyyy`; empate de marca temporal marcado en `POL0000002` |
| B. Reejecución idempotente | Conteos antes = después en `bronze_polizas` (1.508), `silver_polizas` (1.491), `silver_cuarentena` (54), `gold_poliza_360` (2.981 = foto agosto + foto septiembre); logs "ya ingerido (hash …), se omite" |
| C. Lote inválido (200 recaudos negativos) | Recaudos 5.202 de entrada, 216 rechazados (3,8% > umbral 2%) → `CalidadBloqueanteError`, tarea `silver` FAILED, Gold no publicado; recuperación marcando el archivo `RECHAZADO` y reprocesando |
| D. Archivo que incumple el contrato | `siniestros_2026_09_sin_poliza.csv` sin columnas obligatorias → `PENDIENTE_REVISION` y bloqueo hasta decisión del operador (`RECHAZADO`) |

## Idempotencia observada en operación real
El pipeline del corte 2026-08-31 se ejecutó dos veces (2:53 p. m. y 7:34 p. m.) y el job dos veces más
(10:27 p. m. y 10:32 p. m.) sin duplicar registros en ninguna capa.

## Observación menor (deuda técnica)
En el escenario A, `POL0000003` (reenvío idéntico en un archivo con dos columnas adicionales) quedó marcada con la
regla informativa `POL_EMPATE_VERSION`, porque el empate se evalúa con el hash físico de la fila (que incluye las
columnas extra). No afecta datos ni métricas; mejora propuesta: evaluar el empate sobre las columnas del contrato.
