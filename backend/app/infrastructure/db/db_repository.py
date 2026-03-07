from typing import Any, Dict, List, Optional
from sqlalchemy import select,text
from sqlalchemy.dialects.postgresql import insert
from backend.app.infrastructure.db.models import (
    Batch, Document, Party, Line,
    DocumentTax
)
from datetime import time, date, datetime
import re
from sqlalchemy.exc import IntegrityError

def _coerce_time(tval): 
    if not tval:
        return None
    s = str(tval).strip()
    core = s.split("-")[0]  # corta zona si viene
    try:
        hh, mm, ss = core.split(":")
        if "." in ss:
            sec_str, frac = ss.split(".", 1)
            sec = int(sec_str)
            micro = int(round(float("0." + frac) * 1_000_000))
            return time(int(hh), int(mm), sec, micro)
        else:
            return time(int(hh), int(mm), int(ss))
    except Exception:
        # Último recurso: dejar que la DB intente castear si es un formato compatible
        return None

def _coerce_date(dval):
    """
    Acepta 'YYYY-MM-DD' y devuelve datetime.date
    """
    if not dval:
        return None
    if isinstance(dval, date):
        return dval
    s = str(dval).strip()
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _safe_float(x):
    if x is None:
        return None
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None

# --- debajo de helpers existentes (_coerce_time, _coerce_date, _safe_float, etc.)
def _norm_tax_code(code):
    """
    Normaliza tax_code a solo dígitos sin ceros a la izquierda.
    '04' -> '4', '001' -> '1'. Si no hay dígitos, devuelve None.
    """
    if code is None:
        return None
    s = str(code).strip()
    # Mantener solo dígitos (por si llega '04 ' o valores mixtos)
    m = re.findall(r'\d+', s)
    if not m:
        return None
    ds = ''.join(m)
    ds = ds.lstrip('0')
    return ds if ds else None

# --- Helpers: detección de impuestos dinámicos en líneas ---
_FIXED_LINE_KEYS = {
    "Producto", "Cantidad", "Valor Unitario", "Base", "Descuento", "Total"
}

def _iter_line_taxes(l: Dict[str, Any]):
    """
    Recorre un diccionario de línea y devuelve tuplas (tax_name, tax_rate, tax_amount, tax_code).
    tax_rate es fracción (0.19) o None. tax_amount es numérico.
    """
    for k, v in l.items():
        if k in _FIXED_LINE_KEYS:
            continue
        if k.startswith("Total_"):  # Ignorar totales de documento
            continue
        if k.startswith("__code__"):  # Ignorar claves auxiliares como __code__IVA
            continue
        if k.endswith(" (%)"):
            continue
        amount = l.get(k, None)
        if amount is None:
            continue
        pct_key = f"{k} (%)"
        pct_raw = l.get(pct_key, None)

        rate = None
        if pct_raw is not None:
            try:
                r = float(str(pct_raw).replace(",", "."))
                rate = r if r <= 1.0 else r / 100.0
            except Exception:
                rate = None

        amt = _safe_float(amount)
        if amt is None:
            continue
        # Buscamos el code compañero "__code__<NombreImpuesto>"
        code = l.get(f"__code__{k}", None)

        yield k, rate, amt, code

def _iter_document_taxes(md_doc: Dict[str, Any]):
    """
    Busca claves que empiecen por 'Total_' en md_doc y arma (tax_name, tax_rate, tax_amount, tax_code).
    """
    for k, v in md_doc.items():
        if not isinstance(k, str):
            continue
        if not k.startswith("Total_") or k.endswith(" (%)"):
            continue
        if k.endswith("__code") or k.startswith("__code__"):
            continue
        tax_name = k.replace("Total_", "", 1)
        if "__code" in tax_name:
            continue
        tax_name = k.replace("Total_", "", 1)
        amount = v
        pct_key = f"Total_{tax_name} (%)"
        pct_raw = md_doc.get(pct_key, None)

        rate = None
        if pct_raw is not None:
            try:
                r = float(str(pct_raw).replace(",", "."))
                rate = r if r <= 1.0 else r / 100.0
            except Exception:
                rate = None

        amt = _safe_float(amount)
        if amt is None:
            continue
        code = md_doc.get(f"Total_{tax_name}__code", md_doc.get(f"__code__{tax_name}", None))

        yield tax_name, rate, amt, code

def _coerce_batch_status(status: str) -> str:
    s = (status or "").strip().lower()
    if s in {"queued", "upload", "uploaded", "start"}: return "queued"
    if s in {"processing", "running"}: return "processing"
    if s in {"done", "finished", "ok"}: return "done"
    if s in {"error", "failed", "fail"}: return "error"
    return "queued"

def _coerce_doc_type(session, doc_type: str, profile_id: str = None) -> str:
    """
    Busca el tipo de documento primero por código DIAN (01/20/91/etc),
    luego por coincidencia textual contra document_type_map,
    y si falla, cae a heurística normal.
    """
    raw = f"{doc_type or ''} {profile_id or ''}".strip()

    # 1) Intentar detectar código DIAN (2 dígitos)
    import re
    m = re.search(r"\b(01|03|05|20|60|91|92)\b", raw)
    if m:
        code = m.group(1)
        # Buscar en tabla maestra
        q = session.execute(text("""
            SELECT normalized
            FROM document_type_map
            WHERE code = :code
        """), {"code": code}).fetchone()
        if q:
            return q[0]

    # 2) Buscar por descripción aproximada (ILIKE)
    q = session.execute(text("""
        SELECT normalized
        FROM document_type_map
        WHERE lower(description) = lower(:raw)
        OR lower(:raw) LIKE '%' || lower(description) || '%'
        LIMIT 1
    """), {"raw": raw}).fetchone()

    if q:
        return q[0]

    # 3) Heurística fallback
    raw_l = raw.lower()
    if "nota credito" in raw_l: return "credit_note"
    if "nota debito" in raw_l: return "debit_note"
    if "equivalente pos" in raw_l or "pos" in raw_l: return "invoice_pos"
    if "factura" in raw_l: return "invoice"

    return "invoice"

def save_batch(session, filename: str, status: str = "UPLOADED", tenant_id: str = None) -> Batch:
    db_status = _coerce_batch_status(status)  # 'queued' | 'processing' | 'done' | 'error'
    b = Batch(filename=filename, status=db_status, tenant_id=tenant_id)
    session.add(b)
    session.flush()
    return b

def upsert_document_with_lines(
    session,
    batch_id: int,
    md_doc: Dict[str, Any],
    parties: Dict[str, Any],
    lines: List[Dict[str, Any]],
    file_name: Optional[str] = None,
    tenant_id: str = None
) -> int:
    """
    Inserta/actualiza un documento, sus líneas y sus impuestos, en modo idempotente y eficiente.
    - Evita duplicados por (tenant_id, document_id)
    - Inserta líneas en bloque con RETURNING
    - Upsert de impuestos (documento y línea) en lote
    - Savepoint por documento: errores no frenan el resto del batch
    - ✅ Evita duplicar impuestos: si un tax existe por línea, NO inserta el total del documento para ese tax
    """
    if not tenant_id:
        raise ValueError("tenant_id es requerido para insertar el documento.")

    sp = session.begin_nested()  # savepoint por documento

    try:
        doc_id_str = (md_doc.get("DocumentID") or "").strip()
        if not doc_id_str:
            raise ValueError("DocumentID vacío")

        # ---------------- Documento ----------------
        cufe_val = (md_doc.get("CUFE") or "").strip() or None
        cude_val = (md_doc.get("CUDE") or "").strip() or None

        doc_stmt = insert(Document).values(
            batch_id=batch_id,
            tenant_id=tenant_id,
            document_id=doc_id_str,
            document_type=_coerce_doc_type(session, md_doc.get("DocumentType"), md_doc.get("ProfileID")),
            profile_id=md_doc.get("ProfileID"),
            issue_date=_coerce_date(md_doc.get("IssueDate")),
            issue_time=_coerce_time(md_doc.get("IssueTime")),
            cufe=cufe_val,
            cude=cude_val,
            sender_nit=parties.get("SenderNIT"),
            receiver_nit=parties.get("ReceiverNIT"),
            file_name=file_name,
            lm_subtotal_sin_impuestos=_safe_float(md_doc.get("LM_Subtotal_Sin_Impuestos")),
            lm_descuentos=_safe_float(md_doc.get("LM_Descuentos")),
            lm_total_con_impuestos=_safe_float(md_doc.get("LM_Total_Con_Impuestos")),
            lm_total_a_pagar=_safe_float(md_doc.get("LM_Total_A_Pagar")),

        ).on_conflict_do_update(
            index_elements=["tenant_id", "document_id"],
            set_={
                "batch_id": insert(Document).excluded.batch_id,
                "document_type": insert(Document).excluded.document_type,
                "profile_id": insert(Document).excluded.profile_id,
                "issue_date": insert(Document).excluded.issue_date,
                "issue_time": insert(Document).excluded.issue_time,
                "cufe": insert(Document).excluded.cufe,
                "cude": insert(Document).excluded.cude,
                "sender_nit": insert(Document).excluded.sender_nit,
                "receiver_nit": insert(Document).excluded.receiver_nit,
                "file_name": insert(Document).excluded.file_name,
                "lm_subtotal_sin_impuestos": insert(Document).excluded.lm_subtotal_sin_impuestos,
                "lm_descuentos": insert(Document).excluded.lm_descuentos,
                "lm_total_con_impuestos": insert(Document).excluded.lm_total_con_impuestos,
                "lm_total_a_pagar": insert(Document).excluded.lm_total_a_pagar,

            }
        ).returning(Document.id)

        doc_id = session.execute(doc_stmt).scalar_one()
        doc = session.get(Document, doc_id)

        # ---------------- Parties ----------------
        sup_stmt = insert(Party).values(
            document_id=doc.id,
            tenant_id=tenant_id,
            role="supplier",
            razon_social=parties.get("SupplierRazónSocial"),
            nit=parties.get("SenderNIT"),
            correo=parties.get("SupplierCorreo"),
            pais=parties.get("SupplierPaís"),
            municipio=parties.get("SupplierMunicipio")
        ).on_conflict_do_update(
            index_elements=["tenant_id", "document_id", "role"],
            set_={
                "razon_social": insert(Party).excluded.razon_social,
                "nit": insert(Party).excluded.nit,
                "correo": insert(Party).excluded.correo,
                "pais": insert(Party).excluded.pais,
                "municipio": insert(Party).excluded.municipio
            }
        )
        session.execute(sup_stmt)

        cus_stmt = insert(Party).values(
            document_id=doc.id,
            tenant_id=tenant_id,
            role="customer",
            razon_social=parties.get("CustomerRazónSocial"),
            nit=parties.get("ReceiverNIT"),
            correo=parties.get("CustomerCorreo"),
            pais=parties.get("CustomerPaís"),
            municipio=parties.get("CustomerMunicipio")
        ).on_conflict_do_update(
            index_elements=["tenant_id", "document_id", "role"],
            set_={
                "razon_social": insert(Party).excluded.razon_social,
                "nit": insert(Party).excluded.nit,
                "correo": insert(Party).excluded.correo,
                "pais": insert(Party).excluded.pais,
                "municipio": insert(Party).excluded.municipio
            }
        )
        session.execute(cus_stmt)

        # ---------------- Líneas ----------------
        line_rows: List[Dict[str, Any]] = []
        line_index_to_dict: List[Dict[str, Any]] = []

        for idx, l in enumerate(lines, start=1):
            line_rows.append({
                "document_id": doc.id,
                "tenant_id": tenant_id,
                "line_no": idx,
                "producto": l.get("Producto"),
                "cantidad": _safe_float(l.get("Cantidad")),
                "base": _safe_float(l.get("Base")),
                "total": _safe_float(l.get("Total")),
            })
            line_index_to_dict.append(l)

        if line_rows:
            line_stmt = insert(Line).values(line_rows).on_conflict_do_update(
                index_elements=["document_id", "line_no"],
                set_={
                    "producto": insert(Line).excluded.producto,
                    "cantidad": insert(Line).excluded.cantidad,
                    "base": insert(Line).excluded.base,
                    "total": insert(Line).excluded.total,
                }
            ).returning(Line.id)
            returned_ids = [row[0] for row in session.execute(line_stmt).all()]
        else:
            returned_ids = []

        line_ids_by_index = {i + 1: lid for i, lid in enumerate(returned_ids)}

        # ---------------- Impuestos ----------------
        tax_rows: List[Dict[str, Any]] = []

        # Track de impuestos presentes por línea (para NO duplicar con totales del doc)
        line_tax_codes: set[str] = set()
        line_tax_names: set[str] = set()

        # De línea
        for idx, l in enumerate(line_index_to_dict, start=1):
            line_id = line_ids_by_index.get(idx)
            if not line_id:
                continue

            for tax_name, tax_rate, tax_amount, tax_code in _iter_line_taxes(l):
                ncode = _norm_tax_code(tax_code)
                nname = (tax_name or "").strip().lower()

                if ncode:
                    line_tax_codes.add(ncode)
                if nname:
                    line_tax_names.add(nname)

                tax_rows.append({
                    "tenant_id": tenant_id,
                    "document_id": doc.id,
                    "line_id": line_id,
                    "tax_name": tax_name,
                    "tax_rate_key": (None if tax_rate is None else float(tax_rate)),
                    "tax_rate": tax_rate,
                    "tax_amount": tax_amount,
                    "tax_code": ncode,
                })

        # De documento (✅ filtrado anti-duplicados)
        for tax_name, tax_rate, tax_amount, tax_code in _iter_document_taxes(md_doc):
            ncode = _norm_tax_code(tax_code)
            nname = (tax_name or "").strip().lower()

            # Si ese impuesto ya aparece por línea (por código o por nombre), no insertes el total del doc
            if (ncode and ncode in line_tax_codes) or (nname and nname in line_tax_names):
                continue

            tax_rows.append({
                "tenant_id": tenant_id,
                "document_id": doc.id,
                "line_id": None,
                "tax_name": tax_name,
                "tax_rate_key": (None if tax_rate is None else float(tax_rate)),
                "tax_rate": tax_rate,
                "tax_amount": tax_amount,
                "tax_code": ncode,
            })

        if tax_rows:
            tax_stmt = insert(DocumentTax).values(tax_rows).on_conflict_do_update(
                constraint="uq_doc_taxes_upsert_norm",
                set_={
                    "tax_amount": insert(DocumentTax).excluded.tax_amount,
                    "tax_rate": insert(DocumentTax).excluded.tax_rate,
                    "tax_code": insert(DocumentTax).excluded.tax_code,
                }
            )
            session.execute(tax_stmt)

        session.flush()
        sp.commit()
        return doc.id

    except IntegrityError:
        sp.rollback()
        raise
    except Exception:
        sp.rollback()
        raise
