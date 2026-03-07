import inspect
from backend.app.application.processor import FacturaProcessor

def explorar_procesador():
    print("="*50)
    print("🔍 INSPECCIÓN TÉCNICA: FacturaProcessor")
    print("="*50)
    
    # Listar todos los métodos de la clase
    metodos = [m[0] for m in inspect.getmembers(FacturaProcessor, predicate=inspect.isfunction)]
    
    print(f"Se encontraron {len(metodos)} métodos disponibles:")
    for m in metodos:
        if not m.startswith("__"): # Omitir métodos internos de Python
            print(f"  -> {m}")
            
    print("="*50)
    print("💡 Copia esta lista y pégala aquí para ajustar el benchmark.")

if __name__ == "__main__":
    explorar_procesador()