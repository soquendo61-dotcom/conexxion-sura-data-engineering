"""Parámetros de notebooks/jobs (widgets) -> PipelineConfig.

Los tasks del Workflow envían los mismos parámetros a todos los notebooks, incluido
``run_id = {{job.run_id}}``, para que todas las tareas de una ejecución compartan identificador.
"""

from __future__ import annotations

from common.config import PipelineConfig

PARAMETROS = {
    # nombre: (valor por defecto, etiqueta)
    "fecha_corte": ("", "Fecha de corte YYYY-MM-DD (vacío = cierre del mes anterior)"),
    "modo": ("incremental", "Modo: incremental | reproceso"),
    "ambiente": ("dev", "Ambiente: dev | qa | prod"),
    "run_id": ("", "Run ID (lo asigna el job; vacío = se genera)"),
    "job_id": ("", "Job ID (lo asigna el job)"),
    "catalog": ("workspace", "Catálogo Unity Catalog"),
    "schema": ("default", "Schema"),
    "volume": ("volume1", "Volumen de aterrizaje"),
    "raw_subdir": ("raw", "Subcarpeta de archivos en el volumen"),
}


def crear_widgets(dbutils) -> None:
    for nombre, (defecto, etiqueta) in PARAMETROS.items():
        try:
            dbutils.widgets.text(nombre, defecto, etiqueta)
        except Exception:  # el widget ya existe
            pass


def obtener_config(dbutils) -> PipelineConfig:
    crear_widgets(dbutils)
    valores = {nombre: dbutils.widgets.get(nombre) for nombre in PARAMETROS}
    return PipelineConfig.desde_parametros(valores)
