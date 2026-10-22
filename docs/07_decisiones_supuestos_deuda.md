# Decisiones, supuestos y deuda técnica

## Supuestos (declarados explícitamente)

| # | Supuesto | Impacto si no se cumple |
|---|---|---|
| S1 | Las fechas con barras y año al final son `dd/MM/yyyy` (convención colombiana) | 280 fechas cambiarían de mes; debe ratificarse en el contrato |
| S2 | Fecha de corte por defecto = último día del mes anterior a la ejecución; para la prueba, `2026-08-31` (último mes con datos) | – |
| S3 | "Próxima a vencer" = vigente con fin de vigencia dentro de los 30 días siguientes al corte (parametrizable) | – |
| S4 | Costo incurrido = valor pagado + reserva vigente | Corporativamente puede incluir IBNR y gastos de ajuste |
| S5 | Prima devengada pro rata por días sobre [inicio, fin); para canceladas, hasta la fecha efectiva de cancelación | – |
| S6 | Fecha efectiva de cancelación = `fecha_actualizacion` si cae dentro de la vigencia; si no, fin de vigencia | Sobre o subestimar el devengo de canceladas |
| S7 | Prima recaudada = recaudos *Aplicado*; *Pendiente* se reporta aparte; *Reversado* y *Rechazado* no suman | – |
| S8 | Siniestros conocidos al corte = `fecha_aviso <= corte`; se agrupan por mes de ocurrencia | – |
| S9 | Recaudos, siniestros y movimientos cuya póliza no existe **o fue rechazada** no tienen "póliza válida": se excluyen de las métricas y se reportan como discrepancia | – |
| S10 | Mismo contenido con otro ID técnico = reenvío (duplicado de negocio); se conserva el ID menor (original) | – |
| S11 | Una fila idéntica que llega en otro archivo es un reenvío legítimo (no es un error) | – |
| S12 | Moneda única COP | Se requeriría tasa de cambio |

## Decisiones clave

| Decisión | Alternativa considerada | Por qué |
|---|---|---|
| Lógica en módulos `src/` con funciones puras; notebooks delgados | Lógica en celdas | Reutilización, pruebas automáticas, revisión de código |
| Idempotencia por hash de contenido del archivo | Fecha de modificación o nombre | Un reenvío con otro nombre no duplica; los timestamps no son confiables |
| Bronze todo como texto + `_corrupt_record` | Inferir esquema | Fidelidad: no se pierde ni se reinterpreta nada en la entrada |
| Lectura archivo por archivo + `unionByName` | Leer la carpeta completa | Spark tomaría el encabezado del primer archivo y desalinearía columnas si cambia el esquema |
| Silver con MERGE sincronizado desde Bronze completo | Incremental por watermark | Determinístico y simple para el volumen actual; el diseño incremental queda documentado |
| Huérfanos marcados, no rechazados | Enviarlos a cuarentena | Son hechos reales que deben investigarse; rechazarlos ocultaría el problema de negocio |
| Umbrales por regla + severidad | Fallar ante cualquier error | Separar "dato malo puntual" (cuarentena) de "lote roto" (bloqueo) |
| Gold como foto por corte | Sobrescribir | Auditoría de lo publicado, comparación entre meses, backfill aislado |
| Publicación certificada (`ctl_publicaciones` + vistas) | Consumir tablas directamente | Los consumidores nunca ven un corte que no pasó la validación |
| Tablas administradas + liquid clustering | Externas + particiones | Volumen pequeño y Free Edition; evita archivos pequeños |

## Deuda técnica y evolución

| Elemento | Propuesta |
|---|---|
| Silver recomputa desde todo Bronze | Incremental: leer Bronze con `_ingestion_ts > watermark` o Change Data Feed y hacer MERGE solo de las claves afectadas |
| Historia del estado de la póliza | Reconstruir un SCD2 de pólizas a partir de los eventos `CAMBIO_ESTADO`/`CANCELACION`/`RENOVACION` para tener cortes *as-of* exactos en el backfill |
| Umbrales y reglas en código | Externalizar a tablas o YAML de contrato versionados por el data steward, o usar DQX / expectativas de Lakeflow Declarative Pipelines |
| Tablero en notebook | Publicar un AI/BI Dashboard con alertas SQL programadas |
| Costo estimado por duración | Usar `system.billing.usage` y presupuestos con etiquetas por job |
| Pruebas de integración en Databricks | Ejecutar la suite y las postcondiciones en QA desde el pipeline de CD (`bundle run`) |
| Grupos / permisos | Sincronizar grupos de Entra ID por SCIM; el job corre con un service principal (`run_as`) |
| Cuarentena | Flujo de tratamiento (asignación, corrección, reinyección) con estado y SLA |
