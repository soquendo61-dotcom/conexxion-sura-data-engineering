# Guía de reproducción (≤ 10 minutos)

| Min | Paso |
|---|---|
| 0–2 | **Datos:** Catalog → `workspace` → `default` → `volume1` (crear si no existe) → carpeta `raw` → subir los 5 CSV |
| 2–3 | **Código:** Workspace → Create → *Git folder* → `https://github.com/soquendo61-dotcom/conexxion-sura-data-engineering` → rama `feature/data-engineering` (o `dll` tras el merge) |
| 3–4 | `notebooks/00_configuracion_inicial` → **Run all** (valida archivos y crea tablas de control) |
| 4–7 | `notebooks/06_pipeline_completo` → widget `fecha_corte = 2026-08-31` → **Run all** (Bronze, Silver, Gold, validación, publicación, gobierno) |
| 7–9 | `notebooks/07_observabilidad_y_consultas_negocio` → **Run all** (tablero y respuestas a las 6 preguntas de negocio) |
| 9–10 | `notebooks/08_trazabilidad` → **Run all** (registro `POL0000101` de Gold hasta el archivo y la ejecución) |

Opcional:
- `notebooks/09_demo_cuarentena_reproceso_errores`: cambio de esquema, cuarentena, idempotencia y manejo de errores (≈ 5 min).
- `notebooks/11_pruebas_automatizadas`: 20 pruebas con pytest.
- Workflow programado: *Jobs & Pipelines* → `[dev] pipeline_polizas_siniestros` → *Run now*.

Resultado esperado (corte 2026-08-31):
- Silver: 500 clientes, 1.490 pólizas, 4.986 recaudos, 2.489 siniestros, 7.992 movimientos.
- Cuarentena: 53 registros.
- `gold_poliza_360`: 1.490 filas (236 vigentes, 39 próximas a vencer, 669 vencidas, 262 renovadas, 284 canceladas).
- Todas las reconciliaciones y postcondiciones en OK.
