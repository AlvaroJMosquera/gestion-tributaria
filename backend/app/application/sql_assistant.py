# sql_assistant.py
import re
import json
import time
from datetime import date, timedelta

from google import genai
from google.genai import types
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

    # Identificar si de manera explícita piden un año (ej. "del 2025", "en 2024", "para 2025")
    m_year = re.search(r"\b(?:del|de|en|para)\s*(el\s*)?([2][0][0-9]{2})\b", q, re.I)
    if m_year:
        found_year = int(m_year.group(2))
        d1, d2 = _first_day_of_year(found_year), _first_day_next_year(found_year)
        q = re.sub(m_year.group(0), f"entre {d1.isoformat()} y {d2.isoformat()}", q, flags=re.I)

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
            "WHERE LOWER(document_type::text) = 'invoice' "
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
        r"\b(traeme|tráeme|muestrame|muéstrame|las|los|de|del|al|un|una|en|específico|especifico|por|nit|proveedor|provedor|cliente|empresa|facturas|factura|el|la)\b",
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
    matches = PCT_RE.findall(ql)
    if matches:
        rates = [str(int(m) / 100) for m in matches]
        q += f" [hint: buscar en dt.tax_rate usando formato decimal: {', '.join(rates)}]"
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
    DEFAULT_SQL_MODEL = "gemini-2.5-flash"
    DEFAULT_TEXT_MODEL = "gemini-2.5-flash"

    # Seguridad: solo lectura (recomendado)
    ALLOWED_SQL_PREFIX = re.compile(r"(?is)^\s*(SELECT|WITH)\b")
    BLOCKED_PREFIX = re.compile(r"(?is)^\s*(CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\b")

    def __init__(self, tenant_id, model: str | None = None, api_key: str | None = None):
        self.engine = get_engine()
        self.model = model  # si None, se rutea automático por pregunta
        self.tenant_id = tenant_id
        self.api_key = api_key
        self.last_query_context = None
        self.history = []  # Memoria conversacional multil-turno

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
        
        historial_str = ""
        if hasattr(self, 'history') and self.history:
            historial_str = "\n### Historial Conversacional Reciente:\n"
            for past_q in self.history:
                historial_str += f"- Usuario preguntó antes: {past_q}\n"
            historial_str += "\nEl usuario acaba de preguntar lo siguiente (que puede ser una continuación del historial anterior).\n"

        context_info = base_context + historial_str + (f"\n### Contexto anterior extra:\n{context}\n" if context else "")

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

### 📘 DICCIONARIO DE CONOCIMIENTO Y NEGOCIO (¡Usa esto para entender a un humano!)
- **"Ventas", "Facturado", "Ingresos", "He cobrado"**: Se refiere a tus facturas de emisión a CLIENTES. Necesitas hacer un JOIN con `parties p` y filtrar obligatoriamente por `LOWER(p.role::text) = 'customer'`.
- **"Compras", "Gastos", "Pagado"**: Se refiere a facturas emitidas por PROVEEDORES. Necesitas hacer un JOIN con `parties p` y filtrar obligatoriamente por `LOWER(p.role::text) = 'supplier'`.
- **"Cliente", "Comprador"**: Entidad en la tabla `parties` donde `role = 'customer'`.
- **"Proveedor", "Vendedor"**: Entidad en la tabla `parties` donde `role = 'supplier'`.
- **"Ciudad", "Pueblo", "Lugar", "Ubicación"**: Geográficamente se refiere obligatoriamente a la columna `municipio` de la tabla `parties`.
- **"IVA facturado" / "IVA cobrado"**: Impuesto `tax_code = '1'` en ventas (`role = 'customer'`).
- **"IVA descontable" / "IVA pagado"**: Impuesto `tax_code = '1'` en compras (`role = 'supplier'`).

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

### Reglas críticas y Errores Comunes que Debes Evitar:
- RLS ACTIVO: NO agregues filtros por tenant_id, Postgres lo maneja de forma invisible a nivel de sesión.
- El campo `tax_code` es de tipo TEXT, nunca INTEGER. Si omites comillas simples dará error (Operator does not exist: text = integer). SIEMPRE USAR COMILLAS SIMPLES, ejemplo: dt.tax_code = '1'
- Búsqueda de texto (productos/nombres): NUNCA uses coincidencia exacta (`=`). SIEMPRE usa `ILIKE '%texto%'` para evitar que fallen búsquedas parciales. (Ej: `l.producto ILIKE '%cable%'` en vez de `= 'cable'`).
- Enum/estado/tipo: SIEMPRE compara con LOWER(campo::text). Ejemplos:
  • Facturas de venta: LOWER(d.document_type::text) = 'invoice'
  • Notas: LOWER(d.document_type::text) IN ('credit_note','debit_note')
  • Roles party: LOWER(p.role::text) IN ('supplier','customer')
  • Estados batch/usuario: LOWER(campo::text) = 'done'/'activo', etc.
  • ¡Presta MAXIMA atención a CERRAR las comillas simples! Nunca dejes una fecha sin cerrar (Ej. '2025-01-01' y NO '2025-01-01).
  • Si un usuario solicita buscar facturas con un porcentaje de impuesto (ej. "IVA 19%" o "ICUI 20%"), debes buscar en `dt.tax_rate` APLICANDO FORMATO DECIMAL (dividiendo por 100). Ejemplo: para 20% es obligatoriamente `dt.tax_rate = 0.20` (NUNCA 20 entero).
- Totales (Evita Productos Cartesianos o Duplicaciones por JOIN Múltiple):
  • Base subtotal → SUM(l.base)
  • IVA u otros impuestos → SUM(dt.tax_amount)
  • Total a pagar → SUM(d.lm_total_a_pagar)
  • ⚠️ REGLA DE ORO DE MATEMÁTICAS: ¡NUNCA juntas la tabla `lines l` y `document_taxes dt` en un mismo nivel de consulta `FROM d JOIN l JOIN dt` sin subconsultas previas! Esto inflará los números y te equivocarás miserablemente. Si te preguntan "Base + Impuestos" en una misma consulta, DEBES estructurar la respuesta con dos sumatorias aisladas usando dos tablas temporales o subconsultas en un LEFT JOIN pre-agrupado tal como lo haría un humano inteligente y experimentado en SQL, ejemplo: `LEFT JOIN (SELECT document_id, SUM(base) ... GROUP BY document_id) l_agg` y `LEFT JOIN (SELECT document_id, SUM(tax_amount) ... GROUP BY document_id) dt_agg`.
- Impuestos:
  • Si se pide un impuesto, SIEMPRE usa JOIN document_taxes dt ON dt.document_id = d.id (No unas a `lines l` si no es necesario para evitar duplicaciones por producto).
  • Si el usuario menciona ICUI/IBUA/INPP: filtra dt.tax_code = '35', dt.tax_code = '34' o dt.tax_code = '33'.
  • Si se menciona porcentaje (p.ej. 4%), filtra dt.tax_rate = 4.
- JOIN típico montos: documents d JOIN lines l ON l.document_id = d.id
- Fechas en d.issue_date:
  • Este año / este mes usa date_trunc(...)
  • Rangos 'YYYY-MM-DD' respétalos tal cual. (Usa BETWEEN o >= AND <=).
- Evita SELECT * si es posible. 
- Al pedir "facturas", trae `d.id, d.document_id, d.issue_date` en el SELECT.
- Devuelve solo 1 sentencia SQL.

Pregunta: "{question}"

### EJEMPLOS DE RESPUESTA ESPERADA (¡Imita este patrón estrictamente!):
P: ¿Cuánto IVA he cobrado hoy?
R: {{"sql": "SELECT SUM(dt.tax_amount) FROM documents d JOIN document_taxes dt ON dt.document_id = d.id WHERE dt.tax_code = '1' AND LOWER(d.document_type::text) = 'invoice';"}}

P: Facturas del cliente XYZ
R: {{"sql": "SELECT d.id, d.document_id, d.issue_date, d.issue_time, d.sender_nit, d.receiver_nit, d.file_name FROM documents d JOIN parties p ON p.document_id = d.id WHERE LOWER(p.role::text) = 'customer' AND p.razon_social ILIKE '%XYZ%';"}}

P: ¿Qué 5 proveedores me han facturado más en 2025?
R: {{"sql": "SELECT p.razon_social, p.nit, SUM(d.lm_total_a_pagar) AS recaudado FROM documents d JOIN parties p ON p.document_id = d.id WHERE LOWER(p.role::text) = 'supplier' AND date_trunc('year', d.issue_date) = '2025-01-01' GROUP BY p.razon_social, p.nit ORDER BY recaudado DESC LIMIT 5;"}}

Responde en JSON estricto sin incluir texto adicional ni explicaciones:
{{"sql": "<UNA sola sentencia SQL>"}}
""".strip()

        model_to_use = self.model or self.DEFAULT_SQL_MODEL

        def call_ollama_fallback(prompt_text: str) -> str:
            import urllib.request
            import urllib.error
            url = "http://localhost:11434/api/generate"
            data = {
                "model": "llama3.1",
                "prompt": prompt_text,
                "stream": False,
                "format": "json",
                "options": {
                    "temperature": 0.0,
                    "num_predict": 250
                }
            }
            req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'),
                                         headers={'Content-Type': 'application/json'})
            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    res_body = response.read().decode('utf-8')
                    res_json = json.loads(res_body)
                    return res_json.get("response", "")
            except Exception as e:
                print(f"Error en Ollama fallback: {e}")
                return ""

        def check_internet():
            import urllib.request
            try:
                urllib.request.urlopen("http://www.google.com", timeout=2)
                return True
            except Exception:
                return False

        if self.model == "llama3.1":
            print("Usuario seleccionó explícitamente Llama 3.1 local.")
            raw = call_ollama_fallback(prompt)
            try:
                data = json.loads(raw)
                sql = (data.get("sql", "") or "").strip()
            except:
                sql = extract_sql_only(raw)
        elif check_internet():
            print(f"Internet detectado. Usando Gemini ({model_to_use}).")
            
            # Usar API Key de la DB si se proveyó
            if self.api_key:
                client = genai.Client(api_key=self.api_key)
            else:
                client = genai.Client()

            try:
                resp = client.models.generate_content(
                    model=model_to_use,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.0,
                    )
                )
                raw = resp.text
                data = json.loads(raw)
                sql = (data.get("sql", "") or "").strip()
                sql = sql.rstrip('}').rstrip('"').rstrip()
            except Exception:
                try:
                    resp = client.models.generate_content(
                        model=model_to_use,
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            temperature=0.0,
                        )
                    )
                    raw = (resp.text or "").strip()
                    sql = extract_sql_only(raw)
                except Exception as e:
                    print(f"Error con Gemini a pesar de tener internet: {e}. Usando Ollama Llama 3.1 fallback.")
                    raw = call_ollama_fallback(prompt)
                    try:
                        data = json.loads(raw)
                        sql = (data.get("sql", "") or "").strip()
                        sql = sql.rstrip('}').rstrip('"').rstrip()
                    except:
                        sql = extract_sql_only(raw)
        else:
            print("No hay conexión a internet. Usando Ollama (llama3.1) como fallback automático.")
            raw = call_ollama_fallback(prompt)
            try:
                data = json.loads(raw)
                sql = (data.get("sql", "") or "").strip()
                sql = sql.rstrip('}').rstrip('"').rstrip()
            except:
                sql = extract_sql_only(raw)

        if isinstance(sql, str):
            if sql.startswith("```"):
                sql = extract_sql_only(sql)
            
            # Post-procesamiento para errores comunes del Llama 3.1
            # Corrige: dt.tax_code = 1 -> dt.tax_code = '1'
            sql = re.sub(r"(?i)(tax_code\s*=\s*)(\d+)(?!\')", r"\g<1>'\g<2>'", sql)
            
            # Corrige comillas sin cerrar en fechas típicas de Llama (ej. ... BETWEEN '2025-01-01' AND '2025-12-31 GROUP BY ...)
            # Si encuentra algo como 'YYYY-MM-DD seguido inmediatamente de espacio y una palabra clave reservada
            sql = re.sub(r"('\d{4}-\d{2}-\d{2})\s+(GROUP\b|ORDER\b|LIMIT\b|WHERE\b|AND\b|OR\b|HAVING\b)", r"\g<1>' \g<2>", sql, flags=re.IGNORECASE)
            # Elimina JOIN innecesario a `lines l` si hace JOIN con `document_taxes dt` sin usar `l.`
            if re.search(r"(?i)JOIN\s+lines\s+[A-Za-z0-9_]+\s+ON", sql) and not re.search(r"(?i)\bl\.[a-z_]+", sql.replace("l.document_id", "")):
                pass # Lógica más compleja requerida para sanear JOINs, se omitirá por seguridad

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
    def ask(self, question: str, model_option: str = "Automático (Recomendado)") -> str:
        q = question or ""
        q_lower = q.lower().strip()

        # Router automático si no fijas modelo, o si es automático
        if "Llama 3.1" in model_option:
            self.model = "llama3.1"
        elif "Gemini" in model_option:
            self.model = self._pick_model(q_lower)
        else:
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
            with self.engine.connect() as conn:
                rows = conn.execute(text(sql)).mappings().all()
            t1 = time.perf_counter()
            db_ms = int((t1 - t0) * 1000)
            perf = ""

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
                rows = conn.execute(text(sql), {"nit": explicit_nit}).mappings().all()
            t1 = time.perf_counter()
            db_ms = int((t1 - t0) * 1000)
            perf = ""

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
                    rows = conn.execute(text(sql), {"nit": explicit_nit}).mappings().all()
                    t1 = time.perf_counter()
                    db_ms = int((t1 - t0) * 1000)
                    perf = ""

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
                    
                    # Para enviar acciones a la UI
                    options = [
                        {
                            "label": f"{c['razon_social']} (NIT {c['nit']})", 
                            "action": f"tráeme las facturas del nit {c['nit']}"
                        }
                        for c in cand[:5]
                    ]

                    if top.get("sim", 0) >= 0.30:
                        sql = _build_sql_invoices_by_party(nit=top["nit"], name=None, role=role)
                        with self.engine.connect() as conn2:
                            rows = conn2.execute(text(sql), {"nit": top["nit"]}).mappings().all()

                        t1 = time.perf_counter()
                        db_ms = int((t1 - t0) * 1000)
                        perf = ""

                        if rows:
                            encabezado = f"🔎 Asumí que quisiste decir **{top['razon_social']}** (NIT {top['nit']}).\n\n"
                            detalle = f"📊 Encontré {len(rows)} factura(s):\n"
                            for idx, r in enumerate(rows[:10], 1):
                                detalle += f"{idx}. {r['document_id']} | {r['issue_date']} {r['issue_time'] or ''} | sender={r['sender_nit']} receiver={r['receiver_nit']}\n"
                            if len(rows) > 10:
                                detalle += f"... y {len(rows) - 10} más.\n"
                                
                            # Mostramos las facturas, pero damos botones para los DEMÁS prospectos (cand[1:5])
                            other_options = options[1:]
                            if other_options:
                                text_msg = encabezado + detalle + perf + "\n\n¿Buscas a otro? Selecciona aquí:"
                                return {"text": text_msg, "options": other_options}
                            return encabezado + detalle + perf

                        return {
                            "text": f"🔎 No encontré facturas para el candidato ideal. Sugerencias de proveedor/cliente para «{name_candidate}»:\nElige una opción para filtrarla por NIT exacto:",
                            "options": options
                        }

                    return {
                        "text": f"🔎 No encontré «{name_candidate}» unívocamente. ¿Quisiste decir alguno de estos?",
                        "options": options
                    }

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
            perf = ""

            if not rows:
                return "📢 No encontré registros que coincidan con la consulta." + perf

            # Guardar la pregunta en memoria si fue exitosa
            if hasattr(self, 'history'):
                self.history.append(question_norm)
                if len(self.history) > 3:
                    self.history = self.history[-3:]

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
            # Manejo controlado de errores para producción
            print(f"Error interno (no mostrado al usuario): {e}") # Log interno para debug
            return "📢 Lo siento, ocurrió un error interno al intentar consultar tu información. Por favor, intenta reformular tu pregunta de otra forma."
