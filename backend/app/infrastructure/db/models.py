from typing import List, Optional
from datetime import date, time, datetime

from sqlalchemy import (
    BigInteger,
    String,
    Date,
    Time,
    Text,
    ForeignKey,
    Numeric,
    Index,
    TIMESTAMP,
    CheckConstraint,
)
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, ENUM as PGEnum

from backend.app.infrastructure.db.db_config import Base

# =====================================================
# ENUM DEFINITIONS (deben existir en DB; no crear en prod)
# =====================================================
BatchStatus = PGEnum(
    "queued", "processing", "done", "error",
    name="batch_status_enum", schema="public", create_type=False
)

DocType = PGEnum(
    "invoice", "credit_note", "debit_note", "attached", "invoice_pos",
    name="document_type_enum", schema="public", create_type=False
)

PartyRole = PGEnum('supplier','customer', name='party_role_enum', schema='public', create_type=False)

UsuarioStatus = PGEnum(
    "activo", "inactivo", "bloqueado",
    name="usuario_status_enum", schema="public", create_type=False
)

# =========================
# TENANTS / USUARIOS / BATCHES
# =========================
class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(UUID, primary_key=True)
    nombre: Mapped[str] = mapped_column(Text, nullable=False)
    nit: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    creado_en: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP)  # DB default now()

    usuarios: Mapped[List["Usuario"]] = relationship(
        back_populates="tenant",
        cascade="all, delete-orphan"
    )

    batches: Mapped[List["Batch"]] = relationship(
        "Batch",
        back_populates="tenant",
        primaryjoin="Tenant.id == Batch.tenant_id",
        foreign_keys="[Batch.tenant_id]",
        cascade="all, delete-orphan"
    )


class Usuario(Base):
    __tablename__ = "usuarios"

    id: Mapped[str] = mapped_column(UUID, primary_key=True)
    nombre: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    tenant_id: Mapped[str] = mapped_column(UUID, ForeignKey("tenants.id"), nullable=False)
    creado_en: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP)
    status: Mapped[str] = mapped_column(UsuarioStatus, nullable=False, default="activo")
    # Si en DB es timestamptz, puedes usar:
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))

    tenant: Mapped["Tenant"] = relationship(back_populates="usuarios")


class Batch(Base):
    __tablename__ = "batches"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(BatchStatus, nullable=False)
    tenant_id: Mapped[str] = mapped_column(UUID, ForeignKey("tenants.id"), nullable=False)

    tenant: Mapped["Tenant"] = relationship(
        "Tenant",
        back_populates="batches",
        primaryjoin="Batch.tenant_id == Tenant.id",
        foreign_keys="[Batch.tenant_id]"
    )

    documents: Mapped[List["Document"]] = relationship(
        back_populates="batch",
        cascade="all, delete-orphan"
    )


# =========================
# DOCUMENTOS / PARTES / LÍNEAS / ERRORES
# =========================
class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    batch_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("batches.id"), nullable=False)
    document_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    document_type: Mapped[str] = mapped_column(DocType, nullable=False)
    profile_id: Mapped[Optional[str]] = mapped_column(String)
    issue_date: Mapped[Optional[date]] = mapped_column(Date)
    issue_time: Mapped[Optional[time]] = mapped_column(Time)
    cufe: Mapped[Optional[str]] = mapped_column(String)
    cude: Mapped[Optional[str]] = mapped_column(String)
    sender_nit: Mapped[Optional[str]] = mapped_column(String)
    receiver_nit: Mapped[Optional[str]] = mapped_column(String)
    file_name: Mapped[Optional[str]] = mapped_column(String)
    tenant_id: Mapped[str] = mapped_column(UUID, ForeignKey("tenants.id"), nullable=False)

    # ✅ NUEVO: Totales UBL (LegalMonetaryTotal)
    lm_subtotal_sin_impuestos: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    lm_descuentos: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    lm_total_con_impuestos: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    lm_total_a_pagar: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))

    batch: Mapped["Batch"] = relationship(back_populates="documents")
    parties: Mapped[List["Party"]] = relationship(back_populates="document", cascade="all, delete-orphan")
    lines: Mapped[List["Line"]] = relationship(back_populates="document", cascade="all, delete-orphan")
    document_taxes: Mapped[List["DocumentTax"]] = relationship(back_populates="document", cascade="all, delete-orphan")

# Índices útiles para búsquedas por CUFE/CUDE
Index("ix_docs_cufe", Document.cufe)
Index("ix_docs_cude", Document.cude)


class Party(Base):
    __tablename__ = "parties"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    document_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("documents.id"), nullable=False)
    role: Mapped[str] = mapped_column(PartyRole, nullable=False)   # ← enum nuevamente
    razon_social: Mapped[Optional[str]] = mapped_column(String)
    nit: Mapped[Optional[str]] = mapped_column(String)
    correo: Mapped[Optional[str]] = mapped_column(String)
    tenant_id: Mapped[str] = mapped_column(UUID, ForeignKey("tenants.id"), nullable=False)
    document: Mapped["Document"] = relationship(back_populates="parties")


class Line(Base):
    __tablename__ = "lines"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    document_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("documents.id"), nullable=False)
    line_no: Mapped[Optional[int]] = mapped_column(Numeric(10, 0))
    producto: Mapped[Optional[str]] = mapped_column(String)
    cantidad: Mapped[Optional[float]] = mapped_column(Numeric(18, 4))
    base: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    total: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    tenant_id: Mapped[str] = mapped_column(UUID, ForeignKey("tenants.id"), nullable=False)

    document: Mapped["Document"] = relationship(back_populates="lines")


class ErrorLog(Base):
    __tablename__ = "errors"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    batch_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("batches.id"))
    document_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("documents.id"))
    archivo: Mapped[Optional[str]] = mapped_column(String)
    detalle: Mapped[str] = mapped_column(Text, nullable=False)
    # Añadido para consistencia multi-tenant
    tenant_id: Mapped[str] = mapped_column(UUID, ForeignKey("tenants.id"), nullable=False)


# =========================
# IMPUESTOS DINÁMICOS
# =========================
class DocumentTax(Base):
    __tablename__ = "document_taxes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    document_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("documents.id"), nullable=False)
    # Opcional: impuesto a nivel línea
    line_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("lines.id"))
    tax_name: Mapped[str] = mapped_column(Text, nullable=False)
    tax_code: Mapped[Optional[str]] = mapped_column(Text, ForeignKey("tax_schemes.tax_code"))
    tax_rate: Mapped[Optional[float]] = mapped_column(Numeric(12, 6))
    tax_amount: Mapped[float] = mapped_column(Numeric(18, 2), nullable=False)
    tenant_id: Mapped[str] = mapped_column(UUID, ForeignKey("tenants.id"), nullable=False)
    tax_rate_key: Mapped[Optional[float]] = mapped_column(Numeric(12, 6))

    document: Mapped["Document"] = relationship(back_populates="document_taxes")
    line: Mapped[Optional["Line"]] = relationship()
    tax_scheme: Mapped[Optional["TaxScheme"]] = relationship(
        "TaxScheme",
        primaryjoin="foreign(DocumentTax.tax_code)==TaxScheme.tax_code"
    )

    __table_args__ = (
        # El CHECK formal está en DB; este es informativo y protege en ORM si se crea la tabla.
        CheckConstraint(
            "(document_id IS NOT NULL) AND (line_id IS NULL OR line_id IS NOT NULL)",
            name="document_taxes_scope_chk"
        ),
    )


class TaxScheme(Base):
    __tablename__ = "tax_schemes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tax_name: Mapped[str] = mapped_column(Text, nullable=False)
    tax_code: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
