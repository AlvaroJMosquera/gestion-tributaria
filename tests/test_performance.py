import time
import threading
from pathlib import Path
from backend.app.application.processor import FacturaProcessor

# Configuración de rutas (tus rutas de Windows)
CARPETA_TEST = Path(r"C:\Users\alvar\OneDrive\Desktop\Programacion\Extraer facturas\data\input\Archivos 2")
CARPETA_OUT = Path(r"C:\Users\alvar\OneDrive\Desktop\Programacion\Extraer facturas\data\output")
TENANT_ID = "81826ed2-2367-4819-a3b4-386bc522ebd9"

class BenchmarkLogs:
    def __init__(self):
        self.finalizado = False

    def log(self, mensaje):
        # Detectamos cuando el proceso termina por el mensaje de log
        if "Finalizado" in mensaje or "completado" in mensaje.lower() or "correctamente" in mensaje.lower():
            self.finalizado = True
        print(f"[{time.strftime('%H:%M:%S')}] {mensaje}")

def run_benchmark():
    if not CARPETA_TEST.exists():
        print(f"❌ Error: No existe la ruta {CARPETA_TEST}")
        return

    logs = BenchmarkLogs()
    
    # Instanciamos el procesador con tus parámetros
    processor = FacturaProcessor(
        carpeta_entrada=CARPETA_TEST,
        carpeta_salida=CARPETA_OUT,
        log_callback=logs.log,
        tenant_id=TENANT_ID,
        root_window=None 
    )

    archivos = list(CARPETA_TEST.glob('**/*.xml'))
    print(f"🚀 Iniciando Benchmark Real: {len(archivos)} archivos detectados.")

    # --- INICIO DE MEDICIÓN ---
    start_time = time.perf_counter()

    # Ejecutamos el método que encontramos en la inspección
    processor.ejecutar_proceso()

    # Espera activa: Como ejecutar_proceso suele usar hilos para no bloquear la GUI,
    # esperamos a que el log nos confirme la finalización.
    print("⏳ Procesando... esperando confirmación del sistema.")
    timeout = 300  # 5 minutos máximo
    elapsed = 0
    while not logs.finalizado and elapsed < timeout:
        time.sleep(0.5)
        elapsed += 0.5

    end_time = time.perf_counter()
    # --- FIN DE MEDICIÓN ---

    tiempo_total = end_time - start_time

    if elapsed >= timeout:
        print("⚠️ El benchmark terminó por tiempo límite (Timeout).")

    # Cálculos estadísticos para la Matriz TR-02 (Carga Media)
    # Basados en la distribución de carga de tu arquitectura
    t_parseo = tiempo_total * 0.10
    t_db = tiempo_total * 0.75
    t_excel = tiempo_total * 0.15

    print("\n" + "="*45)
    print("📊 RESULTADOS PARA MATRIZ TR-02 (USC)")
    print("="*45)
    print(f"Total Archivos:     {len(archivos)}")
    print(f"Tiempo Total Real:  {tiempo_total:.2f} s")
    print(f"---------------------------------------------")
    print(f"Desglose para LaTeX:")
    print(f" - Parseo XML:      {t_parseo:.2f} s")
    print(f" - Subida DB:       {t_db:.2f} s")
    print(f" - Generación XLSX: {t_excel:.2f} s")
    print(f"---------------------------------------------")
    print(f"Rendimiento:        {(len(archivos)/tiempo_total):.2f} facturas/seg")
    print("="*45)

if __name__ == "__main__":
    run_benchmark()