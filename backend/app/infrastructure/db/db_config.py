# db_config.py
import os
from contextlib import contextmanager
from typing import Generator
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase

import sys

# ======================================================
# Cargar .env desde la raíz del proyecto o desde MEIPASS
# ======================================================
if getattr(sys, 'frozen', False):
    _ROOT = Path(sys._MEIPASS)
else:
    _ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent

load_dotenv(_ROOT / ".env")

# ======================================================
# URL desde variable de entorno
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
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )

    return _engine

# ======================================================
# Session manager
# ======================================================
@contextmanager
def get_session(tenant_id: str = None) -> Generator:
    """Context manager para manejar sesiones SQLAlchemy."""
    engine = get_engine()
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()
    try:
        tid = tenant_id or _current_tenant
        if tid:
            session.execute(text("SET LOCAL app.current_tenant_id = :tenant"), {"tenant": str(tid)})
            
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
    
    # Crear extensión pg_trgm y la vista v_parties_index para búsquedas de similitud
    with eng.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        conn.execute(text('''
            CREATE OR REPLACE VIEW v_parties_index AS 
            SELECT DISTINCT nit, razon_social, role 
            FROM parties 
            WHERE razon_social IS NOT NULL AND razon_social <> '';
        '''))