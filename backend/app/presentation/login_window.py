# login_window.py
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import messagebox
from backend.app.application.auth import login_user
from backend.app.infrastructure.db.db_config import set_current_tenant
from backend.app.application.sql_assistant import SQLAssistant


class LoginWindow:
    """Ventana de login independiente, que devuelve la sesión y tenant_id al cerrarse correctamente."""

    def __init__(self, root):
        self.root = root
        self.root.title("Gestión Tributaria")
        self.root.geometry("420x320")
        self.root.resizable(False, False)
        
        try:
            self.root.eval('tk::PlaceWindow . center')
        except:
            pass

        # Frame principal
        frame = ttk.Frame(root, padding=30)
        frame.pack(fill="both", expand=True)

        ttk.Label(
            frame, 
            text="💼 Gestión Tributaria", 
            font="-size 18 -weight bold", 
            bootstyle="primary"
        ).pack(pady=(0, 10))
        
        ttk.Label(
            frame, 
            text="Inicio de sesión", 
            font="-size 11", 
            bootstyle="secondary"
        ).pack(pady=(0, 18))

        # Campos de login
        ttk.Label(frame, text="Correo electrónico", bootstyle="secondary").pack(anchor="w")
        self.email_var = ttk.StringVar()
        email_entry = ttk.Entry(frame, textvariable=self.email_var, width=35)
        email_entry.pack(fill="x", pady=(0, 10))

        ttk.Label(frame, text="Contraseña", bootstyle="secondary").pack(anchor="w")
        self.pass_var = ttk.StringVar()
        pass_entry = ttk.Entry(frame, textvariable=self.pass_var, show="*", width=35)
        pass_entry.pack(fill="x", pady=(0, 18))

        # Bind Enter para login rápido
        email_entry.bind("<Return>", lambda e: self.do_login())
        pass_entry.bind("<Return>", lambda e: self.do_login())

        # Botón de ingreso
        self.btn_login = ttk.Button(
            frame, 
            text="✅ Ingresar", 
            bootstyle="success", 
            command=self.do_login
        )
        self.btn_login.pack(fill="x", pady=(0, 6))

        # Estado
        self.status_lbl = ttk.Label(frame, text="", bootstyle="danger")
        self.status_lbl.pack()

        # Variables de salida
        self.session = None
        self.tenant_id = None
        self.assistant = None

    def do_login(self):
        """Maneja el proceso de login"""
        email = (self.email_var.get() or "").strip()
        password = (self.pass_var.get() or "").strip()

        if not email or not password:
            messagebox.showwarning("Campos vacíos", "Debes ingresar correo y contraseña.")
            return

        self.status_lbl.configure(text="Verificando credenciales...", bootstyle="info")
        self.btn_login.configure(state="disabled")

        try:
            user = login_user(email, password)
            
            if not user:
                self.status_lbl.configure(text="Credenciales inválidas", bootstyle="danger")
                self.btn_login.configure(state="normal")
                return

            # Login exitoso
            self.session = user
            self.usuario_nombre = user.get("usuario_nombre")
            self.tenant_nombre = user.get("tenant_nombre")

            self.tenant_id = str(user["tenant_id"])
            set_current_tenant(self.tenant_id)
            self.assistant = SQLAssistant(self.tenant_id)

            self.status_lbl.configure(text=f"Bienvenido, {user.get('usuario_nombre', email)}",bootstyle="success")

            # Cerrar ventana después de 600ms
            self.root.after(600, self.close_window)
            
        except Exception as e:
            error_msg = str(e)
            messagebox.showerror("Error de autenticación", f"No fue posible autenticar:\n{error_msg}")
            self.status_lbl.configure(text="Error de conexión", bootstyle="danger")
            self.btn_login.configure(state="normal")

    def close_window(self):
            """Cierra solo la UI de login, pero mantiene vivo el root para reutilizarlo."""
            try:
                # Destruir los widgets del login (frames, labels, entradas, etc.)
                for child in self.root.winfo_children():
                    child.destroy()
                # Salir del mainloop del login
                self.root.quit()
            except Exception as e:
                print(f"Error cerrando ventana: {e}")
