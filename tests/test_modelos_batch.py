import os
import sys
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from backend.app.application.sql_assistant import SQLAssistant
from backend.app.infrastructure.db.db_config import get_engine
from sqlalchemy import text

def main():
    try:
        # Extraer el tenant_id directamente para evitar errores de importación circular
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT tenant_id FROM usuarios WHERE email = 'usuario@prueba.com' LIMIT 1;"))
            row = result.fetchone()
            if not row:
                print("❌ No se encontró el usuario 'usuario@prueba.com'")
                return
            tenant_id = row[0]
            
        print(f"✅ Tenant ID recuperado: {tenant_id}")
        
        # Inicializar ambos asistentes
        assistant_gemini = SQLAssistant(tenant_id=tenant_id, model="gemini-2.5-flash")
        assistant_ollama = SQLAssistant(tenant_id=tenant_id, model="llama3.1")
        
        preguntas = [
            # Filtros por impuesto
            "Tráeme las facturas que tienen retención en la fuente aplicada",
            "Dame las facturas donde el IVA generado supere los $500.000",
            "¿Cuáles facturas tienen INC del 8%?",
            # Consultas por fecha y período
            "¿Cuántas facturas se emitieron en el primer trimestre del 2025?",
            "Dame las facturas del mes de marzo del 2024",
            "¿Cuál fue la semana con mayor facturación en el 2025?",
            # Consultas por proveedor y cliente
            "¿Qué proveedores me han facturado más de 3 veces?",
            "Dame todas las facturas del NIT 900123456",
            "¿Cuál es el proveedor con mayor valor total facturado?",
            # Consultas por producto
            "¿Qué productos tienen un precio unitario mayor a $200.000?",
            "Dame las facturas que contienen el producto ARROZ",
            "¿Cuál es el producto con menor rotación en el 2024?",
            # Totales y resúmenes
            "¿Cuál es el valor total facturado en el 2025?",
            "¿Cuántas facturas tienen un total superior al millón de pesos?",
            "Dame el promedio de valor por factura en el último mes"
        ]

        print("\n" + "="*80)
        print("🚀 INICIANDO BATERÍA DE PRUEBAS COMPARATIVAS")
        print("Modelos: Gemini 2.5 Flash vs Llama 3.1 (Local)")
        print("="*80 + "\n")

        with open(r"C:\Users\alvar\OneDrive\Desktop\Programacion\Extraer facturas\resultados_test_modelos.txt", "w", encoding="utf-8") as f:
            f.write("RESUMEN DE RESULTADOS: Gemini 2.5 Flash vs Llama 3.1\n")
            f.write("="*80 + "\n\n")

            for idx, q in enumerate(preguntas, 1):
                f.write(f"--- PREGUNTA {idx}: {q} ---\n")
                print(f"--- PREGUNTA {idx}: {q} ---")

                # TEST GEMINI
                f.write("> GEMINI 2.5 FLASH:\n")
                try:
                    res_g = assistant_gemini.ask(q, model_option="Solo Gemini (Nube)")
                    f.write(res_g + "\n")
                except Exception as e:
                    f.write(f"ERROR: {str(e)}\n")
                
                f.write("\n")

                # TEST OLLAMA
                f.write("> LLAMA 3.1:\n")
                try:
                    res_o = assistant_ollama.ask(q, model_option="Solo Llama 3.1 (Local)")
                    f.write(res_o + "\n")
                except Exception as e:
                    f.write(f"ERROR: {str(e)}\n")
                
                f.write("\n" + "*"*60 + "\n\n")
                print(f"✅ Completada.")
                time.sleep(1) # pausa entre Pings a GenAI

        print("\n✅ Pruebas finalizadas. Resultados guardados en resultados_test_modelos.txt")

    except Exception as e:
        print(f"Error general: {e}")

if __name__ == "__main__":
    main()
