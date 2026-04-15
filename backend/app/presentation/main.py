# main.py
import platform
import ttkbootstrap as ttk
from ttkbootstrap.constants import *

from backend.app.presentation.login_window import LoginWindow
from backend.app.presentation.gui import ProcessorApp
from backend.app.infrastructure.db.db_config import set_current_tenant
import os
import sys
import subprocess
import atexit

def start_ollama():
    """Inicia el servidor local de Ollama en modo oculto utilizando los archivos empaquetados."""
    try:
        # Determinar el directorio base (compilado vs código fuente)
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
        else:
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
            
        ollama_exe = os.path.join(base_dir, 'installer_assets', 'ollama.exe')
        models_dir = os.path.join(base_dir, 'installer_assets', 'models')
        
        if os.path.exists(ollama_exe):
            print(f"Levantando servicio local Ollama con modelos en: {models_dir}")
            os.environ["OLLAMA_MODELS"] = models_dir
            # creationflags=0x08000000 es CREATE_NO_WINDOW para evitar consola emergente
            proc = subprocess.Popen([ollama_exe, 'serve'], creationflags=0x08000000)
            atexit.register(lambda: proc.terminate())
    except Exception as e:
        print(f"Error al iniciar el servidor Ollama local: {e}")

def _apply_dpi_scaling(win: ttk.Window):
    """Escalado opcional para pantallas HiDPI."""
    try:
        win.tk.call("tk", "scaling", 1.25)
    except Exception:
        pass


def _maximize_window(win: ttk.Window):
    """Inicia la app maximizada de forma portable."""
    try:
        if platform.system() == "Windows":
            win.state("zoomed")
        elif platform.system() == "Linux":
            win.attributes("-zoomed", True)
        else:
            win.geometry("1200x800")
    except Exception:
        win.geometry("1200x800")

def main():
    # Inicia la IA empaquetada si está disponible
    start_ollama()
    
    # ========== 1) Ventana de LOGIN ==========
    login_root = ttk.Window(themename="flatly")
    _apply_dpi_scaling(login_root)
    login_root.title("🔐 Iniciar sesión — Gestión Tributaria")
    login_root.geometry("420x320")   # tamaño cómodo para el login
    login_root.resizable(False, False)

    login_app = LoginWindow(login_root)
    login_root.mainloop()  # se cierra cuando el LoginWindow termina su flujo

    # Si no hay sesión válida, salimos
    if not getattr(login_app, "session", None):
        print("🔒 Inicio cancelado o fallido.")
        return

    # ========== 2) Ventana PRINCIPAL (reutilizando el mismo root) ==========
    main_root = login_root   # 👈 reusamos la misma ventana

    # Restauramos y reconfiguramos la ventana para la app principal
    try:
        main_root.deiconify()        # por si estuviera oculta
    except Exception:
        pass

    main_root.title("💼 Gestión Tributaria | Procesador de Facturas XML")
    main_root.resizable(True, True)
    _maximize_window(main_root)

    # Construir la app principal sobre el mismo root
    app = ProcessorApp(main_root)

    # Pasamos el contexto de login
    app.session = login_app.session
    app.tenant_id = login_app.tenant_id
    app.assistant = login_app.assistant
    app.logged_in = True
    set_current_tenant(app.tenant_id)

    # Actualizamos la barra de estado y habilitamos controles
    usuario = app.session.get("usuario_nombre", app.session.get("email"))
    tenant = app.session.get("tenant_nombre", "")


    def _setup_session():
        try:
            texto = f"Usuario: {usuario}"
            if tenant:
                texto += f" | Tenant: {tenant}"
            app.lbl_session.configure(text=texto, bootstyle="success")
        except Exception as e:
            print(f"Error configurando sesión: {e}")

        if hasattr(app, "_unlock_controls"):
            app._unlock_controls()


    # Aseguramos que el cierre de la X use el método de la app
    try:
        main_root.protocol("WM_DELETE_WINDOW", app._on_close)
    except Exception:
        pass

    # Ejecutar después de que la ventana esté completamente inicializada
    main_root.after(100, _setup_session)

    # Arrancamos la app principal
    main_root.mainloop()
if __name__ == "__main__":
    main()