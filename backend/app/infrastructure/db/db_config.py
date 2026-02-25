# db_config.py
import os
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, DeclarativeBase

# ======================================================
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "❌ Variable DATABASE_URL no encontrada.\n"
        "   Crea un archivo .env en la raíz del proyecto con:\n"
        "   DATABASE_URL=postgresql+psycopg2://usuario:pass@host:5432/db?sslmode=require"
    )
# ======================================================
# Tenant context (variable de sesión en PostgreSQL)
# ======================================================
_current_tenant = None

def set_current_tenant(tenant_id: str):
    """Llamar después del login para fijar tenant_id en la sesión DB."""
    global _current_tenant
    _current_tenant = tenant_id

def _on_connect(dbapi_conn, conn_record):
    """Se ejecuta cada vez que SQLAlchemy abre una conexión nueva."""
    if _current_tenant:
        cur = dbapi_conn.cursor()
        cur.execute("SET app.tenant_id = %s", (_current_tenant,))
        cur.close()

# ======================================================
# SQLAlchemy Base + Engine
# ======================================================
class Base(DeclarativeBase):
    pass

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        print("🔗 Conectando a la base de datos (Supabase)...")

        _engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,  # evita conexiones muertas
            pool_size=5,
            max_overflow=10,
        )

        # 🔑 Aplica tenant automáticamente en cada conexión
        event.listen(_engine, "connect", _on_connect)

    return _engine

# ======================================================
# Session manager
# ======================================================
@contextmanager
def get_session() -> Generator:
    """Context manager para manejar sesiones SQLAlchemy."""
    engine = get_engine()
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def create_all():
    """Crear todas las tablas declaradas en models.py"""
    eng = get_engine()
    Base.metadata.create_all(eng)
