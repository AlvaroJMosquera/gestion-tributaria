# sql_assistant.py
import re
import json
import time
from datetime import date, timedelta

import ollama
from sqlalchemy import text
from backend.app.infrastructure.db.db_config import get_engine

# ============================
# Utilidades de parsing/guardas
# ============================

# Detecta el inicio de una sentencia SQL común (para extracción defensiva)
SQL_START = re.compile(r"(?is)\b(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\b")
# Captura la PRIMERA sentencia completa finalizada en ';' o fin de texto
SQL_STMT = re.compile(r"(?is)\b(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\b.*?(;|$)")

def extract_sql_only(raw: str) -> str:
    """
    Devuelve solo UNA sentencia SQL ejecutable a partir de la salida del LLM.
    Quita fences ``` y se queda con la primera sentencia que empiece en SELECT/WITH/...
    """
    if not isinstance(raw, str):
        return ""
    t = re.sub(r"```(?:sql)?", "", raw, flags=re.I).strip()
    m = SQL_STMT.search(t)
    if m:
        sql = m.group(0).strip()
    else:
        m2 = SQL_START.search(t)
        sql = t[m2.start():].strip() if m2 else t.strip()
    if sql and not sql.endswith(";"):
        sql += ";"
    return sql


def enforce_tenant_guards(sql: str, tenant_id: str) -> str:
    """
    Inyecta filtros tenant_id en tablas con tenant_id si no aparecen explícitamente.
    Normaliza 'tenant_id' sin alias cuando haya joins.
    """
    s = (sql or "").strip()
    low = s.lower()
    must = f"'{tenant_id}'"

    # Caso especial: DELETE/UPDATE directos a usuarios
    if re.search(r"(?i)^\s*delete\s+from\s+usuarios\b", low) or re.search(r"(?i)^\s*update\s+usuarios\b", low):
        combined = f"usuarios.tenant_id = {must}"
        if " where " in low:
            s = re.sub(r"(?is)\bWHERE\b", f"WHERE {combined} AND ", s, count=1)
        else:
            s = s.rstrip(";") + f" WHERE {combined};"
        return s

    # Aliases por defecto (recomendados)
    alias_doc = "d"
    alias_line = "l"
    alias_doc_tax = "dt"
    alias_batch = "b"
    alias_tenant = "t"
    alias_user = "u"
    alias_party = "p"
    alias_err = "e"

    # Detectar alias reales si fueron diferentes
    m_doc = re.search(r"from\s+documents\s+(\w+)", low)
    if m_doc: alias_doc = m_doc.group(1)

    m_line_from = re.search(r"from\s+lines\s+(\w+)", low)
    m_line_join = re.search(r"join\s+lines\s+(\w+)", low)
    if m_line_join: alias_line = m_line_join.group(1)
    if m_line_from: alias_line = m_line_from.group(1)

    m_doc_tax_from = re.search(r"from\s+document_taxes\s+(\w+)", low)
    m_doc_tax_join = re.search(r"join\s+document_taxes\s+(\w+)", low)
    if m_doc_tax_join: alias_doc_tax = m_doc_tax_join.group(1)
    if m_doc_tax_from: alias_doc_tax = m_doc_tax_from.group(1)

    m_batch_from = re.search(r"from\s+batches\s+(\w+)", low)
    m_batch_join = re.search(r"join\s+batches\s+(\w+)", low)
    if m_batch_join: alias_batch = m_batch_join.group(1)
    if m_batch_from: alias_batch = m_batch_from.group(1)

    m_tenant_from = re.search(r"from\s+tenants\s+(\w+)", low)
    m_tenant_join = re.search(r"join\s+tenants\s+(\w+)", low)
    if m_tenant_join: alias_tenant = m_tenant_join.group(1)
    if m_tenant_from: alias_tenant = m_tenant_from.group(1)

    m_user_from = re.search(r"from\s+usuarios\s+(\w+)", low)
    m_user_join = re.search(r"join\s+usuarios\s+(\w+)", low)
    if m_user_join: alias_user = m_user_join.group(1)
    if m_user_from: alias_user = m_user_from.group(1)

    m_party_from = re.search(r"from\s+parties\s+(\w+)", low)
    m_party_join = re.search(r"join\s+parties\s+(\w+)", low)
    if m_party_join: alias_party = m_party_join.group(1)
    if m_party_from: alias_party = m_party_from.group(1)

    m_err_from = re.search(r"from\s+errors\s+(\w+)", low)
    m_err_join = re.search(r"join\s+errors\s+(\w+)", low)
    if m_err_join: alias_err = m_err_join.group(1)
    if m_err_from: alias_err = m_err_from.group(1)

    def has_where_clause(text_lower: str) -> bool:
        return " where " in text_lower

    # Normalizar tenant_id sin alias si hay tablas específicas
    if ("from documents" in low or "join documents" in low):
        s = re.sub(r"(?i)(?<![\w\.])tenant_id\b", f"{alias_doc}.tenant_id", s)
        low = s.lower()
    if ("from lines" in low or "join lines" in low):
        s = re.sub(r"(?i)(?<![\w\.])tenant_id\b", f"{alias_line}.tenant_id", s)
        low = s.lower()
    if ("from document_taxes" in low or "join document_taxes" in low):
        s = re.sub(r"(?i)(?<![\w\.])tenant_id\b", f"{alias_doc_tax}.tenant_id", s)
        low = s.lower()
    if ("from batches" in low or "join batches" in low):
        s = re.sub(r"(?i)(?<![\w\.])tenant_id\b", f"{alias_batch}.tenant_id", s)
        low = s.lower()
    if ("from usuarios" in low or "join usuarios" in low):
        s = re.sub(r"(?i)(?<![\w\.])tenant_id\b", f"{alias_user}.tenant_id", s)
        low = s.lower()
    if ("from parties" in low or "join parties" in low):
        s = re.sub(r"(?i)(?<![\w\.])tenant_id\b", f"{alias_party}.tenant_id", s)
        low = s.lower()
    if ("from errors" in low or "join errors" in low):
        s = re.sub(r"(?i)(?<![\w\.])tenant_id\b", f"{alias_err}.tenant_id", s)
        low = s.lower()

    # Agregar filtros tenant_id por tabla si no están
    filters_to_add = []

    if ("from documents" in low or "join documents" in low) and f"{alias_doc}.tenant_id" not in low:
        filters_to_add.append(f"{alias_doc}.tenant_id = {must}")

    if ("join lines" in low or "from lines" in low) and f"{alias_line}.tenant_id" not in low:
        filters_to_add.append(f"{alias_line}.tenant_id = {must}")

    if ("join document_taxes" in low or "from document_taxes" in low) and f"{alias_doc_tax}.tenant_id" not in low:
        filters_to_add.append(f"{alias_doc_tax}.tenant_id = {must}")

    if ("from batches" in low or "join batches" in low) and f"{alias_batch}.tenant_id" not in low:
        filters_to_add.append(f"{alias_batch}.tenant_id = {must}")

    if ("from tenants" in low or "join tenants" in low) and f"{alias_tenant}.id" not in low:
        filters_to_add.append(f"{alias_tenant}.id = {must}")

    if ("from usuarios" in low or "join usuarios" in low) and f"{alias_user}.tenant_id" not in low:
        filters_to_add.append(f"{alias_user}.tenant_id = {must}")

    if ("from parties" in low or "join parties" in low) and f"{alias_party}.tenant_id" not in low:
        filters_to_add.append(f"{alias_party}.tenant_id = {must}")

    if ("from errors" in low or "join errors" in low) and f"{alias_err}.tenant_id" not in low:
        filters_to_add.append(f"{alias_err}.tenant_id = {must}")

    if filters_to_add:
        combined_filter = " AND ".join(filters_to_add)
        if has_where_clause(low):
            s = re.sub(r"(?is)\bWHERE\b", f"WHERE {combined_filter} AND ", s, count=1)
        else:
            insert_pos = None
            for match in re.finditer(r"(?i)\b(FROM|JOIN)\s+[\w\.]+\s+\w+", s):
                insert_pos = match.end()
            if insert_pos:
                s = s[:insert_pos] + f" WHERE {combined_filter} " + s[insert_pos:]
            else:
                s = s.rstrip(";") + f" WHERE {combined_filter};"

    return s


def _first_day_of_month(y: int, m: int):
    return date(y, m, 1)

def _first_day_next_month(y: int, m: int):
    return date(y + (m // 12), 1 if m == 12 else m + 1, 1)

def _first_day_of_year(y: int):
    return date(y, 1, 1)

def _first_day_next_year(y: int):
    return date(y + 1, 1, 1)

def normalize_temporal_span(q: str) -> str:
    """
    Reescribe frases temporales en español a rangos YYYY-MM-DD.
    No agrega filtros si el usuario no mencionó tiempo alguno.
    """
    if not q:
        return q

    today = date.today()
    y, m = today.year, today.month
    replacements = []

    if re.search(r"\beste\s+año\b", q, re.I):
        d1, d2 = _first_day_of_year(y), _first_day_next_year(y)
        replacements.append((r"\beste\s+año\b", f"entre {d1.isoformat()} y {d2.isoformat()}"))

    if re.search(r"\baño\s+pasado\b", q, re.I):
        d1, d2 = _first_day_of_year(y - 1), _first_day_next_year(y - 1)
        replacements.append((r"\baño\s+pasado\b", f"entre {d1.isoformat()} y {d2.isoformat()}"))

    if re.search(r"\beste\s+mes\b", q, re.I):
        d1, d2 = _first_day_of_month(y, m), _first_day_next_month(y, m)
        replacements.append((r"\beste\s+mes\b", f"entre {d1.isoformat()} y {d2.isoformat()}"))

    if re.search(r"\bmes\s+pasado\b", q, re.I):
        prev_m = 12 if m == 1 else m - 1
        prev_y = y - 1 if m == 1 else y
        d1, d2 = _first_day_of_month(prev_y, prev_m), _first_day_next_month(prev_y, prev_m)
        replacements.append((r"\bmes\s+pasado\b", f"entre {d1.isoformat()} y {d2.isoformat()}"))

    m_last_ndays = re.search(r"\b(últimos|ultimos)\s+(\d+)\s+días\b", q, re.I)
    if m_last_ndays:
        n = int(m_last_ndays.group(2))
        d2 = today + timedelta(days=1)
        d1 = today - timedelta(days=n - 1)
        q = re.sub(
            r"\b(últimos|ultimos)\s+\d+\s+días\b",
            f"entre {d1.isoformat()} y {d2.isoformat()}",
            q,
            flags=re.I,
        )

    if re.search(r"\bay(?:e)r\b", q, re.I):
        d1 = today - timedelta(days=1)
        d2 = today
        q = re.sub(r"\bay(?:e)r\b", f"entre {d1.isoformat()} y {d2.isoformat()}", q, flags=re.I)

    if re.search(r"\bhoy\b", q, re.I):
        d1 = today
        d2 = today + timedelta(days=1)
        q = re.sub(r"\bhoy\b", f"entre {d1.isoformat()} y {d2.isoformat()}", q, flags=re.I)

    for patt, repl in replacements:
        q = re.sub(patt, repl, q, flags=re.I)

    return q


def fix_agg_order_limit(sql: str, tenant_id: str) -> str:
    """
    Si detecta un SELECT con SUM(...) y ORDER BY/LIMIT sin GROUP BY,
    reescribe a un patrón correcto para 'última factura' usando subquery.
    """
    s = (sql or "").strip()
    low = s.lower()

    if "sum(" in low and "order by" in low and "group by" not in low:
        alias_doc = "d"
        m_doc = re.search(r"from\s+documents\s+(\w+)", low)
        if m_doc:
            alias_doc = m_doc.group(1)

        s_no_order = re.sub(r"(?is)\s+order\s+by\s+.*?(?=(limit|;|$))", " ", s)
        s_no_order = re.sub(r"(?is)\s+limit\s+\d+\s*;?", " ", s_no_order).strip().rstrip(";")

        subq = (
            "SELECT id FROM documents "
            f"WHERE tenant_id = '{tenant_id}' AND LOWER(document_type::text) = 'invoice' "
            "ORDER BY issue_date DESC, "
            "COALESCE(issue_time, TIME '00:00:00') DESC, "
            "id DESC "
            "LIMIT 1"
        )

        if re.search(r"(?is)\bwhere\b", s_no_order):
            s_fixed = re.sub(r"(?is)\bwhere\b", f"WHERE {alias_doc}.id = ({subq}) AND ", s_no_order, count=1)
        else:
            s_fixed = re.sub(
                rf"(?is)\bfrom\s+documents\s+{re.escape(alias_doc)}\b",
                f"FROM documents {alias_doc} WHERE {alias_doc}.id = ({subq})",
                s_no_order,
                count=1,
            )

        if not s_fixed.strip().endswith(";"):
            s_fixed += ";"
        return s_fixed

    return s


# ============================
# Diccionario de impuestos UBL <-> Nombre
# ============================

TAX_CODE_TO_NAME = {
    "1":  "IVA (Impuesto al Valor Agregado)",
    "2":  "IC (Impuesto al Consumo Departamental)",
    "3":  "ICA (Impuesto de Industria y Comercio)",
    "4":  "INC Bolsas Plásticas",
    "5":  "INC Gasolina y ACPM (Combustibles)",
    "6":  "Retención en la Fuente Renta",
    "7":  "ReteICA",
    "8":  "ReteIVA",
    "20": "Timbre",
    "21": "Sobretasa Combustibles",
    "22": "INC Carbono",
    "23": "INC Telefonía",
    "24": "INC Licores, Vinos, Aperitivos, Cervezas y Sifones, Cigarrillos y Tabaco",
    "25": "INC (No clasificado - Código 25)",
    "26": "INC (No clasificado - Código 26)",
    "29": "INC Vehículos",
    "30": "Sobretasa Gasolina",
    "31": "Sobretasa ACPM",
    "33": "INPP (Impuesto Nacional a Plásticos de un Solo Uso)",
    "34": "IBUA (Impuesto a Bebidas Ultraprocesadas Azucaradas)",
    "35": "ICUI (Impuesto a Comestibles Ultraprocesados Industriales)",
}
NAME_TO_TAX_CODE = {v.lower(): k for k, v in TAX_CODE_TO_NAME.items()}

TAX_ALIASES = {
    "icui": "35",
    "ibua": "34",
    "inpp": "33",
    "iva":  "1",
    "ica":  "3",
    "ic":   "2",
}

def normalize_tax_term(term: str) -> tuple[str | None, str | None]:
    if not term:
        return (None, None)
    t = str(term).strip().lower()

    tokens = sorted(TAX_ALIASES.keys(), key=len, reverse=True)
    for tok in tokens:
        if re.fullmatch(rf"\b{tok}\b", t):
            code = TAX_ALIASES[tok]
            return (code, TAX_CODE_TO_NAME[code])

    digits = "".join(ch for ch in t if ch.isdigit()).lstrip("0") or None
    if digits and digits in TAX_CODE_TO_NAME:
        return (digits, TAX_CODE_TO_NAME[digits])

    for code, name in TAX_CODE_TO_NAME.items():
        if t in name.lower():
            return (code, name)

    return (None, None)


# ============================
# Búsqueda por proveedor/cliente (NIT o nombre) con “¿quisiste decir…?”
# ============================

NIT_RE = re.compile(r"\b\d{6,12}\b")
PROV_CLI_RE = re.compile(r"\b(proveedor|provedor|cliente)\b", re.I)

def _role_from_text(q_lower: str) -> str | None:
    if "proveedor" in q_lower or "provedor" in q_lower or "vendedor" in q_lower:
        return "supplier"
    if "cliente" in q_lower or "comprador" in q_lower:
        return "customer"
    return None

def _want_invoices_only(q_lower: str) -> bool:
    return re.search(r"\bfacturas?\b", q_lower) is not None

def _extract_name_candidate(q: str) -> str | None:
    s = re.sub(NIT_RE, " ", q)
    s = re.sub(
        r"traeme|tráeme|muestrame|muéstrame|las|los|de|un|una|en|específico|especifico|por|nit|proveedor|provedor|cliente|empresa|facturas|factura|el|la",
        " ",
        s,
        flags=re.I,
    )
    s = re.sub(r"\s+", " ", s).strip()
    return s if s and len(s) >= 3 else None

def _resolve_party_candidates(conn, name: str, role: str | None, limit: int = 5):
    base = """
        SELECT nit, razon_social, role,
               similarity(razon_social, :name) AS sim
        FROM v_parties_index
        WHERE razon_social IS NOT NULL AND razon_social <> ''
    """
    if role in ("supplier", "customer"):
        base += " AND role = :role "
    base += " ORDER BY similarity(razon_social, :name) DESC, razon_social ASC LIMIT :limit;"
    rows = conn.execute(text(base), {"name": name, "role": role, "limit": limit}).mappings().all()
    return [dict(r) for r in rows]

def _build_sql_invoices_by_party(nit: str | None, name: str | None, role: str | None) -> str:
    select_cols = """
        SELECT d.id, d.document_id, d.document_type, d.issue_date, d.issue_time,
               d.sender_nit, d.receiver_nit, d.file_name
        FROM documents d
    """
    where = []
    if nit:
        if role == "supplier":
            where.append("d.sender_nit = :nit")
        elif role == "customer":
            where.append("d.receiver_nit = :nit")
        else:
            where.append("(d.sender_nit = :nit OR d.receiver_nit = :nit)")
    elif name:
        select_cols += " JOIN parties p ON p.document_id = d.id "
        where.append("p.razon_social ILIKE :name_like")
        if role in ("supplier", "customer"):
            where.append("LOWER(p.role::text) = :role")
    else:
        where.append("1=0")

    sql = (
        select_cols
        + " WHERE "
        + " AND ".join(where)
        + " ORDER BY d.issue_date DESC, COALESCE(d.issue_time, TIME '00:00:00') DESC, d.id DESC LIMIT 200;"
    )
    return sql


# ============================
# Intercepts rápidos (sin LLM)
# ============================

def _want_last_invoice(q_lower: str) -> bool:
    return (
        ("ultima" in q_lower or "última" in q_lower or "mas reciente" in q_lower or "más reciente" in q_lower)
        and ("factura" in q_lower or "invoice" in q_lower)
    )

def _build_sql_last_invoice() -> str:
    # tenant_id se inyecta en enforce_tenant_guards
    return (
        "SELECT d.id, d.document_id, d.issue_date, d.issue_time, "
        "d.sender_nit, d.receiver_nit, d.file_name "
        "FROM documents d "
        "WHERE LOWER(d.document_type::text) = 'invoice' "
        "ORDER BY d.issue_date DESC, COALESCE(d.issue_time, TIME '00:00:00') DESC, d.id DESC "
        "LIMIT 1;"
    )


# Pistas de impuesto y porcentaje
PCT_RE = re.compile(r"(\d{1,2})\s*%")

def _inject_tax_hint_in_question(q: str) -> str:
    ql = q.lower()
    for tok, code in sorted(TAX_ALIASES.items(), key=lambda x: len(x[0]), reverse=True):
        if re.search(rf"\b{tok}\b", ql):
            q += f" [hint: impuesto tax_code={code}]"
            break
    m = PCT_RE.search(ql)
    if m:
        pct = int(m.group(1))
        q += f" [hint: tax_rate={pct}]"
    return q


# ============================
# Clase principal
# ============================

class SQLAssistant:
    """
    Asistente SQL:
    - Optimizado para Windows/16GB (keep_alive corto)
    - Intercepts (última factura / NIT / proveedor-cliente) sin LLM
    - Guardas tenant_id automáticas
    - Bloqueo DDL por seguridad; por defecto solo SELECT/WITH
    """
    DEFAULT_SQL_MODEL = "llama3.1"
    DEFAULT_TEXT_MODEL = "gemma3:12b"

    # Seguridad: solo lectura (recomendado)
    ALLOWED_SQL_PREFIX = re.compile(r"(?is)^\s*(SELECT|WITH)\b")
    BLOCKED_PREFIX = re.compile(r"(?is)^\s*(CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\b")

    def __init__(self, tenant_id, model: str | None = None):
        self.engine = get_engine()
        self.model = model  # si None, se rutea automático por pregunta
        self.tenant_id = tenant_id
        self.last_query_context = None

    def _pick_model(self, q_lower: str) -> str:
        sqlish = any(k in q_lower for k in [
            "sql", "consulta", "query", "select", "join", "where", "group by", "order by",
            "iva", "ica", "icui", "ibua", "inpp", "retención", "reteiva", "reteica", "impuesto",
            "ventas", "compras", "facturé", "facturado", "base", "total", "sum(", "count(",
            "factura", "invoice"
        ])
        return self.DEFAULT_SQL_MODEL if sqlish else self.DEFAULT_TEXT_MODEL

    # -----------------------------
    # Generador de SQL con Ollama
    # -----------------------------
    def generate_sql(self, question: str, context: str = None) -> str:
        today_iso = date.today().isoformat()
        base_context = f"\n### Contexto de tiempo:\nHoy: {today_iso}\nZona horaria: America/Bogota\n"
        context_info = base_context + (f"\n### Contexto anterior:\n{context}\n" if context else "")

        prompt = f"""
Eres un asistente SQL experto en PostgreSQL.
Devuelve SOLO UNA sentencia SQL ejecutable (sin explicaciones, sin markdown).

{context_info}

### Alias obligatorios (USALOS SIEMPRE):
- documents d
- lines l
- document_taxes dt
- parties p
- batches b
- tenants t
- usuarios u
- errors e
- tax_schemes ts

### Esquema (resumen):
- tenants(id uuid PK, nombre text, nit text UNIQUE, creado_en timestamp)
- usuarios(id uuid PK, nombre text, email text UNIQUE, password_hash text, tenant_id uuid, creado_en timestamp, status, last_seen_at timestamptz)
- batches(id bigint PK, filename varchar, status, tenant_id uuid)
- documents(id bigint PK, batch_id bigint, document_id text UNIQUE, document_type, issue_date date, issue_time time, cufe, cude,
           sender_nit, receiver_nit, file_name, profile_id text, tenant_id uuid,
           lm_subtotal_sin_impuestos numeric, lm_descuentos numeric, lm_total_con_impuestos numeric, lm_total_a_pagar numeric)
- parties(id bigint PK, document_id bigint, role, razon_social varchar, nit varchar, correo varchar, tenant_id uuid)
- lines(id bigint PK, document_id bigint, line_no numeric, producto varchar, cantidad numeric, base numeric, total numeric, tenant_id uuid)
- tax_schemes(id bigint PK, tax_name text, tax_code text UNIQUE)
- document_taxes(id bigint IDENTITY, document_id bigint, line_id bigint?, tax_name text, tax_code text FK→tax_schemes.tax_code,
                tax_rate numeric, tax_amount numeric, tenant_id uuid, tax_rate_key numeric, line_id_norm bigint)
- errors(id bigint PK, batch_id bigint?, document_id bigint?, archivo varchar, detalle text, tenant_id uuid)

### Reglas críticas:
- NO agregues filtros tenant_id: el sistema los inyecta automáticamente.
- Enum/estado/tipo: SIEMPRE compara con LOWER(campo::text). Ejemplos:
  • Facturas de venta: LOWER(d.document_type::text) = 'invoice'
  • Notas: LOWER(d.document_type::text) IN ('credit_note','debit_note')
  • Roles party: LOWER(p.role::text) IN ('supplier','customer')
  • Estados batch/usuario: LOWER(campo::text) = 'done'/'activo', etc.
- Totales:
  • Total ventas → SUM(l.total)
  • Base → SUM(l.base)
  • Total a pagar (si aplica) → SUM(d.lm_total_a_pagar) o d.lm_total_a_pagar según agrupación
- Impuestos:
  • Si se pide impuesto, usa JOIN document_taxes dt ON dt.document_id = d.id
  • Si el usuario menciona ICUI/IBUA/INPP: filtra dt.tax_code='35'/'34'/'33' respectivamente (prioridad sobre IC/ICA).
  • Si se menciona porcentaje (p.ej. 4%), filtra dt.tax_rate = 4.
- JOIN típico montos: documents d JOIN lines l ON l.document_id = d.id
- Fechas en d.issue_date:
  • Este año / este mes usa date_trunc(...)
  • Rangos 'YYYY-MM-DD' respétalos tal cual.
- Evita SELECT * si es posible.
- Devuelve solo 1 sentencia SQL.

Pregunta: "{question}"

Responde en JSON estricto:
{{"sql": "<UNA sola sentencia SQL>"}}
""".strip()

        model_to_use = self.model or self.DEFAULT_SQL_MODEL

        try:
            resp = ollama.chat(
                model=model_to_use,
                messages=[{"role": "user", "content": prompt}],
                options={"temperature": 0},
                format="json",
                keep_alive="5m",
            )
            raw = resp["message"]["content"]
            data = json.loads(raw)
            sql = (data.get("sql", "") or "").strip()
        except Exception:
            resp = ollama.chat(
                model=model_to_use,
                messages=[{"role": "user", "content": prompt}],
                options={"temperature": 0},
                keep_alive="5m",
            )
            raw = (resp["message"]["content"] or "").strip()
            sql = extract_sql_only(raw)

        if isinstance(sql, str) and sql.startswith("```"):
            sql = extract_sql_only(sql)

        sql = enforce_tenant_guards(sql, str(self.tenant_id))
        sql = fix_agg_order_limit(sql, str(self.tenant_id))
        return sql

    # -----------------------------
    # Ejecutar consulta SQL
    # -----------------------------
    def run_query(self, sql: str):
        cleaned = (sql or "").strip()

        # No permitir múltiples sentencias
        if ";" in cleaned[:-1]:
            raise ValueError("Se detectaron múltiples sentencias en el SQL generado.")

        # Bloquear DDL y comandos peligrosos
        if self.BLOCKED_PREFIX.search(cleaned):
            raise ValueError("SQL bloqueado por seguridad (DDL/comandos peligrosos).")

        # Permitir solo lecturas (recomendado)
        if not self.ALLOWED_SQL_PREFIX.search(cleaned):
            raise ValueError("SQL no permitido. Solo se permite SELECT/WITH en este asistente.")

        with self.engine.connect() as conn:
            result = conn.execute(text(cleaned))
            return [dict(row) for row in result.mappings()]

    # -----------------------------
    # Flujo completo pregunta → SQL → respuesta
    # -----------------------------
    def ask(self, question: str) -> str:
        q = question or ""
        q_lower = q.lower().strip()

        # Router automático si no fijas modelo
        if self.model is None:
            self.model = self._pick_model(q_lower)

        # Saludos / ayuda
        if any(word in q_lower for word in ["hola", "buenas", "qué tal", "que tal", "como estas", "cómo estás", "como estás"]):
            return (
                "👋 Hola! Puedo responder preguntas como:\n"
                "- ¿Cuántas ventas hice en agosto 2025?\n"
                "- ¿Cuánto IVA facturé en julio?\n"
                "- ¿Cuánto ICUI facturé este mes?\n"
                "- Tráeme las facturas de un proveedor específico por nombre o NIT.\n"
                "Escríbeme tu consulta."
            )

        if "que puedes hacer" in q_lower or "qué puedes hacer" in q_lower or "ayuda" in q_lower:
            return (
                "🤖 Genero y ejecuto SQL sobre tu base de facturación. "
                "Calculo ventas, compras, IVA/ICA/ICUI/INC, totales por cliente y más. "
                "También busco facturas por proveedor/cliente usando nombre aproximado o NIT."
            )

        # -------------------------
        # Intercept: última factura (SIN LLM)
        # -------------------------
        if _want_last_invoice(q_lower):
            t0 = time.perf_counter()
            sql = _build_sql_last_invoice()
            sql = enforce_tenant_guards(sql, str(self.tenant_id))
            with self.engine.connect() as conn:
                rows = conn.execute(text(sql)).mappings().all()
            t1 = time.perf_counter()
            db_ms = int((t1 - t0) * 1000)
            perf = f"\n⏱️ LLM: 0 ms · DB: {db_ms} ms · Total: {db_ms} ms\n🧠 SQL:\n{sql}"

            if not rows:
                return "📢 No encontré facturas tipo invoice." + perf

            r = rows[0]
            return (
                "📊 Última factura encontrada:\n"
                f"  • document_id: {r.get('document_id')}\n"
                f"  • issue_date: {r.get('issue_date')}\n"
                f"  • issue_time: {r.get('issue_time')}\n"
                f"  • sender_nit: {r.get('sender_nit')}\n"
                f"  • receiver_nit: {r.get('receiver_nit')}\n"
                f"  • file_name: {r.get('file_name')}\n"
                + perf
            )

        # -------------------------
        # Atajo: mensaje SOLO NIT
        # -------------------------
        only_nit = re.fullmatch(NIT_RE, q.strip())
        if only_nit:
            explicit_nit = only_nit.group(0)
            t0 = time.perf_counter()
            with self.engine.connect() as conn:
                sql = _build_sql_invoices_by_party(nit=explicit_nit, name=None, role=None)
                sql = enforce_tenant_guards(sql, str(self.tenant_id))
                rows = conn.execute(text(sql), {"nit": explicit_nit}).mappings().all()
            t1 = time.perf_counter()
            db_ms = int((t1 - t0) * 1000)
            perf = f"\n⏱️ LLM: 0 ms · DB: {db_ms} ms · Total: {db_ms} ms\n🧠 SQL:\n{sql}"

            if rows:
                out = [
                    f"{i}. {r['document_id']} | {r['issue_date']} {r['issue_time'] or ''} | sender={r['sender_nit']} receiver={r['receiver_nit']}"
                    for i, r in enumerate(rows[:10], 1)
                ]
                more = f"\n... y {len(rows) - 10} más." if len(rows) > 10 else ""
                return f"📊 Encontré {len(rows)} factura(s) para NIT {explicit_nit}:\n\n" + "\n".join(out) + more + perf

            return f"📢 No encontré facturas para NIT {explicit_nit}." + perf

        # -------------------------
        # Intercept: proveedor/cliente con NIT o nombre (SIN LLM)
        # -------------------------
        role = _role_from_text(q_lower)
        nit_match = NIT_RE.search(q)
        explicit_nit = nit_match.group(0) if nit_match else None
        name_candidate = None if explicit_nit else _extract_name_candidate(q)

        if _want_invoices_only(q_lower) and (explicit_nit or PROV_CLI_RE.search(q_lower)):
            t0 = time.perf_counter()
            with self.engine.connect() as conn:
                if explicit_nit:
                    sql = _build_sql_invoices_by_party(nit=explicit_nit, name=None, role=role)
                    sql = enforce_tenant_guards(sql, str(self.tenant_id))
                    rows = conn.execute(text(sql), {"nit": explicit_nit}).mappings().all()
                    t1 = time.perf_counter()
                    db_ms = int((t1 - t0) * 1000)
                    perf = f"\n⏱️ LLM: 0 ms · DB: {db_ms} ms · Total: {db_ms} ms\n🧠 SQL:\n{sql}"

                    if rows:
                        respuesta = f"📊 Encontré {len(rows)} factura(s) para NIT {explicit_nit}:\n\n"
                        for idx, r in enumerate(rows[:10], 1):
                            respuesta += f"{idx}. {r['document_id']} | {r['issue_date']} {r['issue_time'] or ''} | sender={r['sender_nit']} receiver={r['receiver_nit']}\n"
                        if len(rows) > 10:
                            respuesta += f"... y {len(rows) - 10} más.\n"
                        return respuesta + perf
                    return f"📢 No encontré facturas para NIT {explicit_nit}." + perf

                if name_candidate:
                    # Nota: requiere v_parties_index + pg_trgm
                    with self.engine.connect() as c2:
                        cand = _resolve_party_candidates(c2, name_candidate, role)

                    if not cand:
                        return f"📢 No encontré coincidencias para «{name_candidate}». Intenta con parte del nombre o verifica tildes."

                    top = cand[0]
                    sugerencias = "\n".join([
                        f"• {c['razon_social']} (NIT {c['nit']}) sim={round(c.get('sim', 0), 2)}"
                        for c in cand[:5]
                    ])

                    if top.get("sim", 0) >= 0.30:
                        sql = _build_sql_invoices_by_party(nit=top["nit"], name=None, role=role)
                        sql = enforce_tenant_guards(sql, str(self.tenant_id))
                        with self.engine.connect() as conn2:
                            rows = conn2.execute(text(sql), {"nit": top["nit"]}).mappings().all()

                        t1 = time.perf_counter()
                        db_ms = int((t1 - t0) * 1000)
                        perf = f"\n⏱️ LLM: 0 ms · DB: {db_ms} ms · Total: {db_ms} ms\n🧠 SQL:\n{sql}"

                        if rows:
                            encabezado = f"🔎 ¿Quisiste decir **{top['razon_social']}** (NIT {top['nit']})?\n\n"
                            detalle = f"📊 Encontré {len(rows)} factura(s):\n"
                            for idx, r in enumerate(rows[:10], 1):
                                detalle += f"{idx}. {r['document_id']} | {r['issue_date']} {r['issue_time'] or ''} | sender={r['sender_nit']} receiver={r['receiver_nit']}\n"
                            if len(rows) > 10:
                                detalle += f"... y {len(rows) - 10} más.\n"
                            return encabezado + "Sugerencias:\n" + sugerencias + "\n\n" + detalle + perf

                        return f"🔎 Sugerencias de proveedor/cliente para «{name_candidate}»:\n{sugerencias}"

                    return f"🔎 ¿Quisiste decir…?\n{sugerencias}"

        # -------------------------
        # Flujo estándar con LLM
        # -------------------------
        question_norm = normalize_temporal_span(q)
        question_norm = _inject_tax_hint_in_question(question_norm)

        t0 = time.perf_counter()
        sql = self.generate_sql(question_norm, self.last_query_context)
        t1 = time.perf_counter()

        try:
            rows = self.run_query(sql)
            t2 = time.perf_counter()

            gen_ms = int((t1 - t0) * 1000)
            db_ms = int((t2 - t1) * 1000)
            total_ms = int((t2 - t0) * 1000)
            perf = f"\n⏱️ LLM: {gen_ms} ms · DB: {db_ms} ms · Total: {total_ms} ms\n🧠 SQL:\n{sql}"

            if not rows:
                return "📢 No encontré registros que coincidan con la consulta." + perf

            if len(rows) == 1 and "receiver_nit" in rows[0]:
                self.last_query_context = f"Última consulta sobre receiver_nit={rows[0]['receiver_nit']}"
            elif len(rows) == 1 and "sender_nit" in rows[0]:
                self.last_query_context = f"Última consulta sobre sender_nit={rows[0]['sender_nit']}"

            if len(rows) == 1:
                respuesta = "📊 Resultado:\n"
                for k, v in rows[0].items():
                    respuesta += f"  • {k}: {v}\n"
                return respuesta.strip() + perf

            respuesta = f"📊 Encontré {len(rows)} resultado(s):\n\n"
            for idx, row in enumerate(rows[:10], 1):
                respuesta += f"Registro {idx}:\n"
                for k, v in row.items():
                    respuesta += f"  • {k}: {v}\n"
                respuesta += "\n"
            if len(rows) > 10:
                respuesta += f"... y {len(rows) - 10} registro(s) más.\n"

            return respuesta.strip() + perf

        except Exception as e:
            return f"❌ Error ejecutando la consulta: {e}\n\n🧠 SQL:\n{sql}"
