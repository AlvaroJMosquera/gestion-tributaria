from backend.app.infrastructure.db.db_config import create_all, get_session
from backend.app.infrastructure.db.db_dedup import apply_unique_indexes
# Crear tablas si no existen
create_all()

# Crear índices únicos en CUFE/CUDE
with get_session() as s:
    apply_unique_indexes(s)

print("✅ Tablas, impuestos dinámicos y índices listos en Supabase")
