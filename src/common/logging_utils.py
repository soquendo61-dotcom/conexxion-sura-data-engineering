"""Logging estructurado para procesos productivos (en lugar de print)."""

from __future__ import annotations

import logging
import sys

_FORMATO = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_configurado = False


def get_logger(nombre: str, run_id: str | None = None) -> logging.LoggerAdapter:
    """Logger con el identificador de ejecución en cada mensaje para facilitar la trazabilidad."""
    global _configurado
    if not _configurado:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMATO))
        raiz = logging.getLogger("sura_de")
        raiz.handlers = [handler]
        raiz.setLevel(logging.INFO)
        raiz.propagate = False
        # Reducir ruido de librerías
        logging.getLogger("py4j").setLevel(logging.WARNING)
        _configurado = True
    logger = logging.getLogger(f"sura_de.{nombre}")
    return _RunAdapter(logger, {"run_id": run_id or "-"})


class _RunAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"run_id={self.extra['run_id']} | {msg}", kwargs
