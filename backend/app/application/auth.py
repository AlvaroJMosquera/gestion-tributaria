# auth.py
from sqlalchemy import text
from backend.app.infrastructure.db.db_config import get_engine
from datetime import datetime

def login_user(email, password):
    engine = get_engine()
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            row = conn.execute(
                text("""
                    SELECT 
                        u.id,
                        u.tenant_id,
                        u.email,
                        u.nombre AS usuario_nombre,
                        t.nombre AS tenant_nombre
                    FROM public.usuarios u
                    JOIN public.tenants t ON t.id = u.tenant_id
                    WHERE u.email = :email
                    AND u.password_hash = crypt(:password, u.password_hash)
                """),
                {"email": email.lower().strip(), "password": password}
            ).mappings().fetchone()


            if row is None:
                trans.rollback()
                return None
            
            # Actualizar last_seen_at
            conn.execute(
                text("UPDATE usuarios SET last_seen_at = :now WHERE id = :id"),
                {"now": datetime.utcnow(), "id": row["id"]}
            )
            trans.commit()

            return {
                "user_id": row["id"],
                "tenant_id": row["tenant_id"],
                "email": row["email"],
                "usuario_nombre": row["usuario_nombre"],
                "tenant_nombre": row["tenant_nombre"]
                }

        except Exception:
            trans.rollback()
            raise