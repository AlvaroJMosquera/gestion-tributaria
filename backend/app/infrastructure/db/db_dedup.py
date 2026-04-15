from typing import Any, Dict, List, Tuple
from sqlalchemy import select, text,insert
from sqlalchemy.exc import IntegrityError,SQLAlchemyError
from backend.app.infrastructure.db.models import Document, Line, DocumentTax, Party
from backend.app.infrastructure.db.db_repository import upsert_document_with_lines

CREATE_UNIQUE_INDEXES_SQL = [
    "DROP INDEX IF EXISTS ux_documents_cufe;",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_cufe ON documents (cufe, tenant_id) WHERE cufe IS NOT NULL AND cufe <> '';",
    "DROP INDEX IF EXISTS ux_documents_cude;",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_cude ON documents (cude, tenant_id) WHERE cude IS NOT NULL AND cude <> '';",
    "DROP INDEX IF EXISTS ux_documents_docid_tenant;",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_docid_tenant ON documents (document_id, tenant_id) WHERE document_id IS NOT NULL AND document_id <> '';"
]

def apply_unique_indexes(session):
    for sql in CREATE_UNIQUE_INDEXES_SQL:
        session.execute(text(sql))
    session.commit()

from sqlalchemy import select, text, insert, or_

def _find_existing(session, cufe, cude, document_id, tenant_id: str):
    conditions = []
    if cufe:
        conditions.append(Document.cufe == cufe)
    if cude:
        conditions.append(Document.cude == cude)
    if document_id:
        conditions.append(Document.document_id == document_id)
        
    if not conditions:
        return None
        
    query = select(Document).where(
        or_(*conditions)
    )
    
    # Return the first one that matches any of the criteria
    doc = session.execute(query).scalars().first()
    return doc

def upsert_document_with_lines_idempotent(
    session, batch_id, md_doc, parties, lines, file_name=None, tenant_id=None
):
    """Inserta documento y líneas solo si no existe; si falla, no tumba el lote."""
    cufe = md_doc.get("CUFE"); cude = md_doc.get("CUDE"); doc_id = md_doc.get("DocumentID")

    existing = _find_existing(session, cufe, cude, doc_id, tenant_id)
    if existing:
        return existing.id, True  # ya estaba

    # SAVEPOINT por documento
    with session.begin_nested():  # <-- SAVEPOINT
        try:
            new_id = upsert_document_with_lines(session, batch_id, md_doc, parties, lines, file_name, tenant_id)
            return new_id, False
        except IntegrityError:
            # carrera / duplicado: reconsulta y retorna como duplicado
            session.rollback()
            existing = _find_existing(session, cufe, cude, doc_id, tenant_id)
            if existing:
                return existing.id, True
            raise
        except SQLAlchemyError:
            # Cualquier otra falla solo revierte este doc y deja seguir
            session.rollback()
            raise