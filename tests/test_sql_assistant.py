# tests/test_sql_assistant.py
import pytest

from backend.app.application.sql_assistant import (
    extract_sql_only,
    enforce_tenant_guards,
    fix_agg_order_limit,
)

TENANT = "00000000-0000-0000-0000-000000000001"


def test_extract_sql_only_strips_fences_and_keeps_first_statement():
    raw = """```sql
SELECT * FROM documents;
DROP TABLE documents;
```"""
    sql = extract_sql_only(raw)
    assert sql.strip().lower().startswith("select")
    assert "drop table" not in sql.lower()
    assert sql.strip().endswith(";")


def test_extract_sql_only_adds_semicolon_if_missing():
    raw = "SELECT 1"
    sql = extract_sql_only(raw)
    assert sql.strip().endswith(";")


def test_enforce_tenant_guards_adds_filter_for_documents_when_missing():
    q = "SELECT d.id, d.document_id FROM documents d ORDER BY d.id DESC;"
    guarded = enforce_tenant_guards(q, TENANT).lower()
    assert "where" in guarded
    assert f"d.tenant_id = '{TENANT}'".lower() in guarded


def test_enforce_tenant_guards_preserves_existing_tenant_filter():
    q = f"SELECT * FROM documents d WHERE d.tenant_id = '{TENANT}' ORDER BY d.id DESC;"
    guarded = enforce_tenant_guards(q, TENANT)
    # no debe duplicar filtros
    assert guarded.lower().count("tenant_id") == q.lower().count("tenant_id")


def test_enforce_tenant_guards_adds_filters_for_joins_lines_and_docs():
    q = """
    SELECT d.document_id, l.producto
    FROM documents d
    JOIN lines l ON l.document_id = d.id
    """
    guarded = enforce_tenant_guards(q, TENANT).lower()
    assert f"d.tenant_id = '{TENANT}'".lower() in guarded
    assert f"l.tenant_id = '{TENANT}'".lower() in guarded


def test_fix_agg_order_limit_rewrites_bad_sum_order_limit():
    q = "SELECT SUM(d.lm_total_a_pagar) FROM documents d ORDER BY d.issue_date DESC LIMIT 1;"
    fixed = fix_agg_order_limit(q, TENANT).lower()
    # Debe construir subquery para obtener "último documento"
    assert "select id from documents" in fixed
    assert f"tenant_id = '{TENANT}'".lower() in fixed