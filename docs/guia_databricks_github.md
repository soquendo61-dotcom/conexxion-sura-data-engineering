# Guía paso a paso: Databricks Free Edition + GitHub

> Ninguna credencial (contraseña o token) se escribe en notebooks ni en el repositorio.
> Los tokens se pegan únicamente en la configuración de Databricks / GitHub.

## 1. Cuenta y workspace
1. Ir a `https://www.databricks.com/learn/free-edition` → **Sign up**. Registrarse con correo o cuenta Google/Microsoft.
2. Databricks crea un workspace personal con cómputo **serverless**. Ya existen el catálogo `workspace` y el schema `default`.

## 2. Volumen y archivos fuente
1. Menú izquierdo **Catalog** → `workspace` → `default` → **Create → Volume** → nombre `volume1`, tipo *Managed*.
2. Abrir `volume1` → **Create directory** → `raw`.
3. Entrar a `raw` → **Upload to this volume** → subir `clientes.csv`, `polizas.csv`, `recaudos.csv`, `siniestros.csv`, `movimientos.csv`.
4. Resultado: `/Volumes/workspace/default/volume1/raw/<archivo>.csv`.

> Convención: cada archivo debe empezar por el nombre del dataset (`polizas*.csv`). Entregas futuras se suben
> con otro nombre (p. ej. `polizas_2026_09.csv`); el pipeline solo procesa archivos con contenido nuevo (hash).

## 3. Integración con GitHub
1. GitHub → **Settings → Developer settings → Personal access tokens → Fine-grained tokens** → acceso al repositorio
   `conexxion-sura-data-engineering` con permiso *Contents: Read and write*.
2. Databricks → avatar → **Settings → Linked accounts → Git integration** → proveedor **GitHub** → *Link Git account*
   (o usuario + token).
3. **Workspace → Create → Git folder** → URL `https://github.com/soquendo61-dotcom/conexxion-sura-data-engineering`.
4. En el Git folder, selector de rama → `feature/data-engineering`. Todo el desarrollo se hace desde esta carpeta.

## 4. Primera ejecución
1. Abrir `notebooks/00_configuracion_inicial` → **Run all** (crea/verifica el volumen, valida archivos y crea tablas de control).
2. Abrir `notebooks/06_pipeline_completo` → en el widget `fecha_corte` escribir `2026-08-31` → **Run all**.
3. Abrir `notebooks/07_observabilidad_y_consultas_negocio` → **Run all** (tablero y preguntas de negocio).
4. Abrir `notebooks/08_trazabilidad` → **Run all** (trazabilidad de `POL0000101`).
5. (Evidencia de errores y cuarentena) `notebooks/09_demo_cuarentena_reproceso_errores` → **Run all**.
6. (Pruebas en Databricks) `notebooks/11_pruebas_automatizadas` → **Run all**.

## 5. Workflow programado (día 5 de cada mes, 06:00 a. m. hora Colombia)

**Opción A – Interfaz (recomendada en Free Edition):** **Jobs & Pipelines → Create → Job**
1. Nombre: `[dev] pipeline_polizas_siniestros`.
2. **Job parameters** (Edit parameters): `fecha_corte` = *(vacío)*, `modo` = `incremental`, `ambiente` = `dev`,
   `catalog` = `workspace`, `schema` = `default`, `volume` = `volume1`, `run_id` = `{{job.run_id}}`, `job_id` = `{{job.id}}`.
3. Tareas (tipo *Notebook*, fuente *Workspace*, cómputo *Serverless*), cada una dependiente de la anterior:

| Task key | Notebook | Depende de | Reintentos | Timeout |
|---|---|---|---|---|
| `bronze` | `notebooks/01_bronze` | – | 2 (5 min) | 30 min |
| `silver` | `notebooks/02_silver` | bronze | 1 (5 min) | 45 min |
| `gold` | `notebooks/03_gold` | silver | 2 (5 min) | 30 min |
| `validar_publicar` | `notebooks/04_validar_publicar` | gold | 1 (2 min) | 20 min |
| `gobierno_y_vistas` | `notebooks/05_gobierno_y_vistas` | validar_publicar | 2 (1 min) | 15 min |

4. **Schedules & Triggers → Add trigger → Scheduled** → *Advanced* → cron `0 0 6 5 * ?` → zona **(UTC-05:00) America/Bogota**.
5. **Job notifications** → *On failure* y *Duration warning* → su correo. **Max concurrent runs** = 1.
6. Para la evidencia: **Run now with different parameters** → `fecha_corte = 2026-08-31`.

**Opción B – Asset Bundle:** con Databricks CLI configurado: `databricks bundle deploy -t dev` y
`databricks bundle run -t dev pipeline_polizas_siniestros` (definición en `databricks.yml` y `resources/jobs/`).

## 6. Accesos para el equipo evaluador
1. **Usuarios del workspace:** avatar → **Settings → Identity and access → Users → Add user** →
   `yesidmontoya@sura.com.co` y `fjcarvajal@sura.com.co`.
   *Si Free Edition no permite agregar usuarios externos, documentarlo como limitación y compartir evidencias (capturas,
   export de notebooks .dbc, resultados) más el repositorio.*
2. **Notebooks / carpeta Git:** clic derecho sobre la carpeta → **Share** → evaluadores con permiso **Can view** (o *Can run*).
3. **Job:** Job → **Permissions** → evaluadores **Can view**.
4. **SQL warehouse** (si se usa): **SQL Warehouses → Permissions → Can use**.
5. **Unity Catalog:** los `GRANT` de solo lectura se aplican automáticamente en `05_gobierno_y_vistas`
   (`USE CATALOG`, `USE SCHEMA`, `SELECT`, `EXECUTE`, `READ VOLUME`). Verificar en Catalog → `default` → **Permissions**.
6. **GitHub:** repositorio → **Settings → Collaborators → Add people** → invitar a los evaluadores (rol *Read*) o
   dejar el repositorio público. Compartir el enlace del Pull Request.
7. **Verificación:** pedir a un evaluador que abra el enlace o revisar en *Permissions* que los permisos aparezcan.

## 7. Pull Request final
GitHub → **Pull requests → New** → base `dll` ← compare `feature/data-engineering` → título
"Solución de ingeniería de datos end-to-end" → descripción (resumen, evidencias, última ejecución exitosa) → **Create**.

## 8. Exportar notebooks para la entrega
Workspace → carpeta `notebooks` → menú `⋮` → **Export → DBC archive** (y/o *IPython notebook*). Adjuntar el `.dbc`.

## Limitaciones de Free Edition y equivalente corporativo

| Free Edition | Equivalente corporativo recomendado |
|---|---|
| Un solo workspace y catálogo `workspace` | Workspaces por ambiente (DEV/QA/PROD) y catálogos aislados (`dev_seguros`, `qa_seguros`, `prod_seguros`) |
| Solo cómputo serverless con cuotas | Serverless jobs o job clusters con políticas de cómputo y presupuestos |
| Sin *external locations* propias | ADLS Gen2 con *external locations* y *storage credentials* (managed identity) |
| Usuarios/grupos de cuenta limitados | Grupos sincronizados desde Entra ID (SCIM) y service principals |
| Streaming solo `availableNow` | Streaming continuo / Lakeflow Declarative Pipelines |
| Sin system tables de facturación garantizadas | `system.billing.usage`, `system.access.audit`, `system.lakeflow.*` |
