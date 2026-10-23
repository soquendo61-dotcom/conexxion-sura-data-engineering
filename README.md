# Conexxion Sura – Solución de Ingeniería de Datos para Pólizas y Siniestros

Solución end-to-end sobre **Databricks Free Edition + Delta Lake + Unity Catalog** que lleva cinco fuentes operacionales
(clientes, pólizas, recaudos, siniestros y movimientos) hasta productos de datos gobernados, con arquitectura
**Medallion**, reglas de calidad con cuarentena, reconciliación, orquestación programada, observabilidad,
seguridad de datos personales y CI/CD.

```
 Archivos CSV ──► Volumen UC ──► BRONZE ──► SILVER ──────────────► GOLD ──────────────► Consumo
 (5 fuentes)      volume1/raw    copia fiel   tipado, dedup,        poliza_360           vistas certificadas
                                 + metadatos  reglas, cuarentena,   siniestralidad       BI / analítica / IA
                                 + hash       reconciliación, SCD2  discrepancias        tablero de control
                                                                    calidad_pipeline
```

## Contenido

| Ruta | Qué contiene |
|---|---|
| `src/common` | Configuración parametrizada, logging, utilidades Delta (escritura idempotente, MERGE), tablas de control |
| `src/contracts` | **Data contracts** de las 5 fuentes (esquema, tipos, obligatoriedad, PII, alias por cambios de esquema, owner) |
| `src/ingestion` | Ingesta a Bronze: hash de archivo, metadatos, idempotencia, validación de contrato, evolución de esquema |
| `src/transformations` | Estandarización, deduplicación, Silver, SCD2 y productos Gold (funciones puras y probadas) |
| `src/quality` | Motor de reglas declarativas (severidad, acción, umbral) y catálogo de reglas por dataset |
| `src/pipeline` | Tareas del Workflow con bitácora, ejecución de SQL versionado y parámetros de notebooks |
| `notebooks` | Notebooks delgados (orquestan funciones de `src/`), evidencias, trazabilidad, demo de errores, streaming |
| `resources/jobs` | Definición del Workflow programado (día 5, 06:00 COT) como Databricks Asset Bundle |
| `resources/sql` | Gobierno Unity Catalog (máscaras, tags, grants), vistas certificadas, observabilidad, consultas de negocio |
| `tests` | 20 pruebas automatizadas (unitarias + integración en memoria Bronze→Silver→Gold) |
| `.github/workflows` | CI (lint + pruebas en cada PR) y CD (promoción DEV → QA → PROD con aprobación) |
| `docs` | Cuestionario, arquitectura, perfilamiento, modelo y calidad, operación, decisiones, opcionales, guías |
| `scripts/validacion_local.py` | Ejecuta Silver/Gold en memoria con los CSV reales (sin Databricks) |

## Documentación

1. [Respuestas del cuestionario](docs/01_cuestionario.md)
2. [Guía Databricks + GitHub paso a paso](docs/guia_databricks_github.md)
3. [Arquitectura objetivo y de la implementación](docs/03_arquitectura.md) · [diagrama](docs/architecture/diagrama_arquitectura.md)
4. [Perfilamiento de fuentes, hallazgos y riesgos](docs/04_perfilamiento_fuentes.md)
5. [Modelo de datos, productos Gold y reglas de calidad](docs/05_modelo_datos_y_calidad.md)
6. [Orquestación, operación y observabilidad](docs/06_operacion_observabilidad.md)
7. [Decisiones, supuestos y deuda técnica](docs/07_decisiones_supuestos_deuda.md)
8. [Opcionales: tiempo casi real e IA generativa](docs/08_opcionales_streaming_ia.md)
9. [Guía de reproducción en 10 minutos](docs/09_guia_reproduccion_10min.md)
10. [Evidencias de ejecución y última ejecución exitosa](docs/10_evidencias_ejecucion.md)

## Ejecución rápida en Databricks

1. Subir los 5 CSV a `/Volumes/workspace/default/volume1/raw/`.
2. Crear un *Git folder* con este repositorio y cambiar a la rama `feature/data-engineering`.
3. Ejecutar `notebooks/00_configuracion_inicial` (Run all).
4. Ejecutar `notebooks/06_pipeline_completo` con `fecha_corte = 2026-08-31` (Run all).
5. Revisar `notebooks/07_observabilidad_y_consultas_negocio` y `notebooks/08_trazabilidad`.
6. Crear el Workflow programado (ver guía, sección 5) o desplegarlo con `databricks bundle deploy -t dev`.

## Ejecución local de pruebas (opcional)

Requiere Python 3.10–3.12 y Java 17.

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements-dev.txt     # Linux/Mac: .venv/bin/pip
.venv/Scripts/python -m pytest -v
.venv/Scripts/python scripts/validacion_local.py --ruta "<carpeta con los CSV>" --fecha-corte 2026-08-31
```

## Convención de objetos (Unity Catalog `workspace.default`)

| Prefijo | Capa / uso | Ejemplos |
|---|---|---|
| `bronze_<dataset>` | Copia fiel + metadatos | `bronze_polizas` |
| `silver_<dataset>` | Estándar, deduplicado, validado | `silver_polizas`, `silver_cuarentena`, `silver_clientes_historia` |
| `gold_<producto>` | Productos de datos | `gold_poliza_360`, `gold_siniestralidad_mensual`, `gold_calidad_pipeline`, `gold_discrepancias` |
| `ctl_<objeto>` | Control operativo | `ctl_ejecuciones`, `ctl_archivos`, `ctl_metricas_dataset`, `ctl_watermarks`, `ctl_publicaciones` |
| `vw_gold_*_certificada` | Consumo del último corte publicado | `vw_gold_poliza_360_certificada` |
| `vw_obs_*` | Observabilidad | `vw_obs_ejecuciones`, `vw_obs_frescura_sla` |
| `fn_mask_*` | Funciones de enmascaramiento PII | `fn_mask_documento` |

## Ramas y control de versiones

- `dll`: rama principal (protegida, solo vía Pull Request con CI en verde).
- `feature/data-engineering`: desarrollo con commits progresivos de propósito claro.
- No se versionan datos crudos, secretos, checkpoints ni artefactos temporales (`.gitignore` + validación en CI).
