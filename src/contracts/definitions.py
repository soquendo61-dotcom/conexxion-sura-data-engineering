"""Data contracts de las fuentes.

Cada contrato define, entre productor (sistema fuente) y consumidor (este pipeline):
- esquema esperado: columnas, tipo destino, obligatoriedad;
- semántica: descripción de cada columna y clave de negocio;
- clasificación de datos sensibles (PII) para gobierno y enmascaramiento;
- tolerancia a evolución de esquema: alias de columnas renombradas;
- operación: owner, frecuencia y SLA de entrega.

Los contratos se validan en cada ejecución (ver ``transformations.standardize.validar_contrato``).
Ver documentación generada en ``docs/05_modelo_datos_y_calidad.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

TIPOS_SOPORTADOS = ("string", "date", "decimal", "int")


@dataclass(frozen=True)
class Columna:
    nombre: str
    tipo: str = "string"
    requerida: bool = False
    descripcion: str = ""
    pii: str | None = None  # None | "ALTA" | "MEDIA"
    aliases: tuple[str, ...] = ()
    dominio: tuple[str, ...] | None = None  # valores permitidos (validez)

    def __post_init__(self) -> None:
        if self.tipo not in TIPOS_SOPORTADOS:
            raise ValueError(f"Tipo no soportado en contrato: {self.tipo}")


@dataclass(frozen=True)
class ContratoDataset:
    nombre: str
    clave: str
    columnas: tuple[Columna, ...]
    descripcion: str
    sistema_origen: str
    owner: str
    frecuencia: str = "mensual"
    columna_version: str | None = None  # define la versión más reciente de una clave
    columnas_orden_desempate: tuple[str, ...] = ()
    columnas_huella_excluir: tuple[str, ...] = ()  # columnas fuera de la huella de negocio
    columnas_monetarias: tuple[str, ...] = ()
    version: str = "1.0.0"
    sla_entrega: str = "Archivo disponible a más tardar el día 4 de cada mes 23:59 (COT)"
    tags: dict = field(default_factory=dict)

    @property
    def nombres(self) -> list[str]:
        return [c.nombre for c in self.columnas]

    @property
    def requeridas(self) -> list[str]:
        return [c.nombre for c in self.columnas if c.requerida]

    @property
    def mapa_alias(self) -> dict[str, str]:
        return {a: c.nombre for c in self.columnas for a in c.aliases}

    def columnas_tipo(self, tipo: str) -> list[str]:
        return [c.nombre for c in self.columnas if c.tipo == tipo]

    @property
    def columnas_pii(self) -> dict[str, str]:
        return {c.nombre: c.pii for c in self.columnas if c.pii}

    @property
    def columnas_huella(self) -> list[str]:
        """Columnas que definen la 'huella de negocio' para detectar el mismo hecho con otro ID."""
        excluir = {self.clave, "origen", *self.columnas_huella_excluir}
        return [c for c in self.nombres if c not in excluir]


SEGMENTOS = ("Personas", "Pyme", "Corporativo", "Independiente")
TIPOS_DOC = ("CC", "CE", "NIT", "PP", "TI")
ESTADOS_POLIZA = ("Vigente", "Vencida", "Cancelada", "Renovada")
PRODUCTOS = ("Autos", "Salud", "Vida", "Hogar", "Empresarial", "Responsabilidad civil")
ESTADOS_RECAUDO = ("Aplicado", "Pendiente", "Reversado", "Rechazado")
MEDIOS_PAGO = ("Tarjeta", "PSE", "Debito automatico", "Transferencia", "Efectivo")
ESTADOS_SINIESTRO = ("Abierto", "En investigacion", "Reactivado", "Cerrado")
ENTIDADES_EVENTO = ("POLIZA", "SINIESTRO", "RECAUDO", "CLIENTE")
EVENTOS_POR_ENTIDAD = {
    "POLIZA": ("CREACION", "CAMBIO_ESTADO", "RENOVACION", "CANCELACION"),
    "SINIESTRO": ("AVISO", "ACTUALIZACION_RESERVA", "PAGO", "CIERRE", "REAPERTURA"),
    "RECAUDO": ("PAGO_APLICADO", "PAGO_PENDIENTE", "PAGO_REVERSADO"),
    "CLIENTE": ("ACTUALIZACION_DATOS", "CAMBIO_SEGMENTO"),
}

CLIENTES = ContratoDataset(
    nombre="clientes",
    clave="cliente_id",
    descripcion="Maestro de clientes (una fila por cliente o versión del cliente).",
    sistema_origen="CRM",
    owner="Dominio Clientes – Data Owner CRM",
    columna_version="fecha_actualizacion",
    columnas=(
        Columna("cliente_id", requerida=True, descripcion="Identificador técnico del cliente"),
        Columna("tipo_documento", requerida=True, dominio=TIPOS_DOC, descripcion="Tipo de documento"),
        Columna("numero_documento", requerida=True, pii="ALTA", aliases=("documento",),
                descripcion="Número de documento de identidad"),
        Columna("fecha_nacimiento", tipo="date", pii="MEDIA", descripcion="Fecha de nacimiento"),
        Columna("ciudad", descripcion="Ciudad de residencia"),
        Columna("segmento", requerida=True, dominio=SEGMENTOS, descripcion="Segmento comercial"),
        Columna("correo", pii="ALTA", aliases=("email",), descripcion="Correo electrónico"),
        Columna("telefono", pii="ALTA", aliases=("celular",), descripcion="Teléfono de contacto"),
        Columna("fecha_actualizacion", tipo="date", requerida=True, descripcion="Fecha de la versión"),
        Columna("origen", descripcion="Sistema origen"),
    ),
)

POLIZAS = ContratoDataset(
    nombre="polizas",
    clave="poliza_id",
    descripcion="Pólizas (una fila por póliza o movimiento de estado).",
    sistema_origen="CORE_POLIZAS",
    owner="Dominio Pólizas – Data Owner Core Pólizas",
    columna_version="fecha_actualizacion",
    columnas_monetarias=("prima",),
    columnas=(
        Columna("poliza_id", requerida=True, descripcion="Identificador de la póliza"),
        Columna("cliente_id", requerida=True, descripcion="Tomador / cliente"),
        Columna("producto", requerida=True, dominio=PRODUCTOS, descripcion="Producto / ramo"),
        Columna("sucursal", requerida=True, descripcion="Sucursal de expedición"),
        Columna("fecha_inicio_vigencia", tipo="date", requerida=True, aliases=("inicio_vigencia",),
                descripcion="Inicio de vigencia"),
        Columna("fecha_fin_vigencia", tipo="date", requerida=True, aliases=("fin_vigencia",),
                descripcion="Fin de vigencia"),
        Columna("estado", requerida=True, dominio=ESTADOS_POLIZA, descripcion="Estado reportado por la fuente"),
        Columna("prima", tipo="decimal", requerida=True, aliases=("valor_prima", "prima_emitida"),
                descripcion="Prima emitida de la vigencia (COP)"),
        Columna("moneda", descripcion="Moneda"),
        Columna("canal", descripcion="Canal de venta"),
        Columna("fecha_actualizacion", tipo="date", requerida=True, descripcion="Fecha de la versión"),
        Columna("origen", descripcion="Sistema origen"),
    ),
)

RECAUDOS = ContratoDataset(
    nombre="recaudos",
    clave="recaudo_id",
    descripcion="Recaudos de prima (una fila por transacción).",
    sistema_origen="PAGOS",
    owner="Dominio Financiero – Data Owner Recaudo",
    columna_version="fecha_ingesta",
    columnas_monetarias=("valor",),
    columnas_huella_excluir=("fecha_ingesta",),
    columnas=(
        Columna("recaudo_id", requerida=True, descripcion="Identificador de la transacción"),
        Columna("poliza_id", requerida=True, descripcion="Póliza a la que se aplica"),
        Columna("fecha_pago", tipo="date", requerida=True, descripcion="Fecha del pago"),
        Columna("valor", tipo="decimal", requerida=True, aliases=("valor_pago",), descripcion="Valor (COP)"),
        Columna("medio_pago", dominio=MEDIOS_PAGO, descripcion="Medio de pago"),
        Columna("estado_transaccion", requerida=True, dominio=ESTADOS_RECAUDO, descripcion="Estado"),
        Columna("referencia_bancaria", pii="MEDIA", descripcion="Referencia bancaria"),
        Columna("fecha_ingesta", tipo="date", descripcion="Fecha en que la fuente registró el pago"),
        Columna("origen", descripcion="Sistema origen"),
    ),
)

SINIESTROS = ContratoDataset(
    nombre="siniestros",
    clave="siniestro_id",
    descripcion="Siniestros (una fila por siniestro o actualización).",
    sistema_origen="CORE_SINIESTROS",
    owner="Dominio Indemnizaciones – Data Owner Siniestros",
    columna_version="fecha_actualizacion",
    columnas_monetarias=("valor_pagado", "valor_reserva"),
    columnas=(
        Columna("siniestro_id", requerida=True, descripcion="Identificador del siniestro"),
        Columna("poliza_id", requerida=True, descripcion="Póliza afectada"),
        Columna("fecha_ocurrencia", tipo="date", requerida=True, descripcion="Fecha de ocurrencia"),
        Columna("fecha_aviso", tipo="date", requerida=True, descripcion="Fecha de aviso"),
        Columna("tipo_siniestro", descripcion="Tipo / cobertura afectada"),
        Columna("estado", requerida=True, dominio=ESTADOS_SINIESTRO, descripcion="Estado del siniestro"),
        Columna("valor_pagado", tipo="decimal", requerida=True, aliases=("pagado",), descripcion="Pagado (COP)"),
        Columna("valor_reserva", tipo="decimal", requerida=True, aliases=("reserva",),
                descripcion="Reserva vigente (COP)"),
        Columna("ciudad_ocurrencia", descripcion="Ciudad de ocurrencia"),
        Columna("fecha_actualizacion", tipo="date", requerida=True, descripcion="Fecha de la versión"),
        Columna("origen", descripcion="Sistema origen"),
    ),
)

MOVIMIENTOS = ContratoDataset(
    nombre="movimientos",
    clave="evento_id",
    descripcion="Eventos de negocio sobre pólizas, siniestros, recaudos y clientes (una fila por evento).",
    sistema_origen="BUS_EVENTOS",
    owner="Arquitectura de Integración – Bus de eventos",
    columna_version="fecha_ingesta",
    columnas_orden_desempate=("secuencia",),
    columnas_huella_excluir=("fecha_ingesta",),
    columnas=(
        Columna("evento_id", requerida=True, descripcion="Identificador del evento"),
        Columna("entidad", requerida=True, dominio=ENTIDADES_EVENTO, descripcion="Entidad afectada"),
        Columna("id_entidad", requerida=True, descripcion="Identificador de la entidad afectada"),
        Columna("tipo_evento", requerida=True, descripcion="Tipo de evento"),
        Columna("fecha_evento", tipo="date", requerida=True, descripcion="Fecha de negocio del evento"),
        Columna("fecha_ingesta", tipo="date", requerida=True, descripcion="Fecha de recepción en el bus"),
        Columna("payload", descripcion="Atributos del evento en JSON"),
        Columna("secuencia", tipo="int", descripcion="Secuencia del evento (desempate)"),
        Columna("origen", descripcion="Sistema origen"),
    ),
)

CONTRATOS: dict[str, ContratoDataset] = {
    c.nombre: c for c in (CLIENTES, POLIZAS, RECAUDOS, SINIESTROS, MOVIMIENTOS)
}

# Orden de procesamiento: primero padres (integridad referencial) y luego hijos.
ORDEN_PROCESAMIENTO = ("clientes", "polizas", "recaudos", "siniestros", "movimientos")
