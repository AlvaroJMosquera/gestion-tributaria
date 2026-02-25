# tests/test_tenant_guards.py
from backend.app.application.sql_assistant import enforce_tenant_guards

TENANT = "00000000-0000-0000-0000-000000000001"


def test_guards_for_parties_alias_detection():
    q = "SELECT p.nit, p.razon_social FROM parties p;"
    guarded = enforce_tenant_guards(q, TENANT).lower()
    assert "where" in guarded
    assert f"p.tenant_id = '{TENANT}'".lower() in guarded


def test_guards_for_batches_alias_detection():
    q = "SELECT b.id, b.status FROM batches b;"
    guarded = enforce_tenant_guards(q, TENANT).lower()
    assert f"b.tenant_id = '{TENANT}'".lower() in guarded


def test_delete_from_usuarios_forced_where():
    q = "DELETE FROM usuarios;"
    guarded = enforce_tenant_guards(q, TENANT).lower()
    assert "where" in guarded
    assert f"usuarios.tenant_id = '{TENANT}'".lower() in guarded


def test_update_usuarios_forced_where():
    q = "UPDATE usuarios SET status='inactivo';"
    guarded = enforce_tenant_guards(q, TENANT).lower()
    assert "where" in guarded
    assert f"usuarios.tenant_id = '{TENANT}'".lower() in guarded