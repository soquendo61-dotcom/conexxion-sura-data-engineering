"""Configuración parametrizada del pipeline.

Todos los valores que cambian entre ejecuciones o ambientes (fecha de corte, modo, catálogo,
schema, volumen, ruta de aterrizaje) se concentran aquí para evitar rutas y fechas rígidas
en el código de transformación.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Mapping

MODOS_VALIDOS = ("incremental", "reproceso")
AMBIENTES_VALIDOS = ("dev", "qa", "prod")


class ConfiguracionInvalidaError(ValueError):
    """Parámetros de ejecución inválidos (se falla rápido, antes de tocar datos)."""


def ultimo_dia_mes_anterior(hoy: date) -> date:
    """Fecha de corte por defecto: cierre del mes anterior a la ejecución.

    El job corre el día 5 de cada mes, por lo que procesa el mes calendario ya cerrado.
    """
    return hoy.replace(day=1) - timedelta(days=1)


def _parse_fecha(valor: str) -> date:
    try:
        return datetime.strptime(valor.strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ConfiguracionInvalidaError(
            f"fecha_corte='{valor}' no es válida. Use el formato YYYY-MM-DD (ej. 2026-08-31)."
        ) from exc


@dataclass(frozen=True)
class PipelineConfig:
    """Parámetros de una ejecución del pipeline."""

    fecha_corte: date = field(default_factory=lambda: ultimo_dia_mes_anterior(date.today()))
    modo: str = "incremental"
    ambiente: str = "dev"
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    catalog: str = "workspace"
    schema: str = "default"
    volume: str = "volume1"
    raw_subdir: str = "raw"
    ruta_raw_override: str | None = None  # permite pruebas locales o rutas alternas
    pipeline: str = "polizas_siniestros"
    job_id: str | None = None
    dias_proxima_a_vencer: int = 30
    dias_llegada_tardia: int = 30

    def __post_init__(self) -> None:
        if self.modo not in MODOS_VALIDOS:
            raise ConfiguracionInvalidaError(
                f"modo='{self.modo}' no es válido. Valores permitidos: {MODOS_VALIDOS}."
            )
        if self.ambiente not in AMBIENTES_VALIDOS:
            raise ConfiguracionInvalidaError(
                f"ambiente='{self.ambiente}' no es válido. Valores permitidos: {AMBIENTES_VALIDOS}."
            )
        if self.dias_proxima_a_vencer <= 0:
            raise ConfiguracionInvalidaError("dias_proxima_a_vencer debe ser positivo.")

    # ------------------------------------------------------------------ rutas y nombres
    @property
    def ruta_volumen(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def ruta_raw(self) -> str:
        return self.ruta_raw_override or f"{self.ruta_volumen}/{self.raw_subdir}"

    def tabla(self, nombre: str) -> str:
        """Nombre completamente calificado (catalog.schema.tabla) gobernado por Unity Catalog."""
        return f"{self.catalog}.{self.schema}.{nombre}"

    @property
    def fecha_corte_str(self) -> str:
        return self.fecha_corte.isoformat()

    def as_dict(self) -> dict:
        d = asdict(self)
        d["fecha_corte"] = self.fecha_corte_str
        return d

    # ------------------------------------------------------------------ construcción
    @classmethod
    def desde_parametros(cls, params: Mapping[str, str | None]) -> "PipelineConfig":
        """Crea la configuración a partir de parámetros de job/widgets (todos llegan como texto).

        Valores vacíos toman el valor por defecto, lo que permite que el job programado
        no envíe fecha_corte y se calcule automáticamente.
        """

        def valor(nombre: str) -> str | None:
            v = params.get(nombre)
            return v.strip() if isinstance(v, str) and v.strip() else None

        kwargs: dict = {}
        if valor("fecha_corte"):
            kwargs["fecha_corte"] = _parse_fecha(valor("fecha_corte"))
        for nombre in ("modo", "ambiente", "run_id", "catalog", "schema", "volume",
                       "raw_subdir", "pipeline", "job_id"):
            if valor(nombre):
                kwargs[nombre] = valor(nombre).lower() if nombre in ("modo", "ambiente") else valor(nombre)
        if valor("ruta_raw"):
            kwargs["ruta_raw_override"] = valor("ruta_raw")
        if valor("dias_proxima_a_vencer"):
            kwargs["dias_proxima_a_vencer"] = int(valor("dias_proxima_a_vencer"))
        return cls(**kwargs)
