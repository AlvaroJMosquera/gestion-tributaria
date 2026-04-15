# gui.py
# -*- coding: utf-8 -*-
import platform
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import filedialog, Text, messagebox, END
from pathlib import Path
import threading
from backend.app.application.processor import FacturaProcessor
from backend.app.application.sql_assistant import SQLAssistant
import traceback
import sys

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


def _apply_dpi_scaling(win: ttk.Window):
    """Escalado suave para pantallas HiDPI (no afecta pantallas normales)."""
    try:
        win.tk.call("tk", "scaling", 1.25)
    except Exception:
        pass
    # Wrapper para capturar el stack trace completo

class ProcessorApp:
    def __init__(self, root: ttk.Window):
        self.root = root
        self.root.title("Gestión Tributaria")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.resizable(True, True)

        _apply_dpi_scaling(self.root)
        _maximize_window(self.root)

        # Estado general
        self.carpeta_entrada: Path | None = None
        self.carpeta_salida: Path | None = None
        self.processor: FacturaProcessor | None = None
        self.processing = False

        # Sesión
        self.session = None
        self.tenant_id: str | None = None
        self.assistant: SQLAssistant | None = None
        self.logged_in = False

        # Referencias explícitas a botones
        self.btn_buscar_entrada = None
        self.btn_buscar_salida = None

        # Construir interfaz
        self._build_ui()

    # ----------------------------
    # Activar controles tras login
    # ----------------------------
    def _unlock_controls(self):
        """Habilita los controles de procesamiento y chat después del login."""
        def _do_unlock():
            try:
                self.ent_entrada.configure(state="normal")
                self.ent_salida.configure(state="normal")

                # Botones "Buscar"
                if self.btn_buscar_entrada:
                    self.btn_buscar_entrada.configure(state="normal")
                if self.btn_buscar_salida:
                    self.btn_buscar_salida.configure(state="normal")

                # Modo
                self.rb_excel.configure(state="normal")
                self.rb_both.configure(state="normal")

                # Chat IA
                self.chat_entry.configure(state="normal")
                self.btn_enviar.configure(state="normal")

                # Mensaje de bienvenida al habilitar el chat
                self._append_chat(
                    "🤖 Asistente: Listo, ya puedes hacer consultas sobre tus impuestos y facturas procesadas."
                )

                # Verifica rutas
                self._verificar_rutas()
            except Exception as e:
                print(f"Error en _do_unlock: {e}")
        
        # Asegurar que se ejecuta en el hilo principal
        self.root.after(0, _do_unlock)

    # ----------------------------
    # Construcción de UI principal
    # ----------------------------
    def _build_ui(self):
        # Header
        header = ttk.Frame(self.root, padding=(14, 12))
        header.pack(fill="x")
        ttk.Label(
            header,
            text="🧾 Gestión Tributaria",
            font="-size 16 -weight bold",
            bootstyle="primary",
        ).pack(side="left")
        ttk.Label(
            header,
            text="Extracción de impuestos y retenciones",
            bootstyle="secondary",
        ).pack(side="left", padx=(12, 0))

        # Barra de sesión
        self.statusbar = ttk.Frame(self.root, padding=(14, 4))
        self.statusbar.pack(fill="x")
        self.lbl_session = ttk.Label(
            self.statusbar, text="Estado: No autenticado", bootstyle="danger"
        )
        self.lbl_session.pack(side="left")

        # Notebook principal (solo una pestaña de Procesamiento)
        self.nb = ttk.Notebook(self.root, bootstyle="secondary")
        self.nb.pack(fill="both", expand=True, padx=10, pady=10)

        self.tab_proc = ttk.Frame(self.nb)
        self.nb.add(self.tab_proc, text="📦 Procesamiento")

        self._build_proc_tab(self.tab_proc)

    # ----------------------------
    # Pestaña Procesamiento (con PanedWindow)
    # ----------------------------
    def _build_proc_tab(self, frame):
        # PanedWindow vertical: 0) Config, 1) Log, 2) Chat
        pw = ttk.Panedwindow(frame, orient="vertical", bootstyle="secondary")
        pw.pack(fill="both", expand=True)

        # ---------- Panel 0: Configuración ----------
        conf_outer = ttk.Labelframe(
            pw,
            text="⚙️ Configuración de Proceso",
            bootstyle="secondary",
            padding=14,
        )
        conf_outer.columnconfigure(1, weight=1)

        ttk.Label(
            conf_outer, text="📥 Carpeta de Entrada:", bootstyle="primary"
        ).grid(row=0, column=0, sticky=W, pady=5)
        self.entrada_var = ttk.StringVar()
        self.ent_entrada = ttk.Entry(
            conf_outer, textvariable=self.entrada_var, state="disabled"
        )
        self.ent_entrada.grid(row=0, column=1, sticky="ew", padx=5)
        self.btn_buscar_entrada = ttk.Button(
            conf_outer,
            text="Buscar",
            bootstyle="info",
            command=self.seleccionar_entrada,
            state="disabled",
        )
        self.btn_buscar_entrada.grid(row=0, column=2, padx=2)

        ttk.Label(
            conf_outer, text="💾 Carpeta de Salida:", bootstyle="primary"
        ).grid(row=1, column=0, sticky=W, pady=5)
        self.salida_var = ttk.StringVar()
        self.ent_salida = ttk.Entry(
            conf_outer, textvariable=self.salida_var, state="disabled"
        )
        self.ent_salida.grid(row=1, column=1, sticky="ew", padx=5)
        self.btn_buscar_salida = ttk.Button(
            conf_outer,
            text="Buscar",
            bootstyle="info",
            command=self.seleccionar_salida,
            state="disabled",
        )
        self.btn_buscar_salida.grid(row=1, column=2, padx=2)

        self.modo_var = ttk.StringVar(value="excel_y_bd")
        modo_frame = ttk.Labelframe(
            conf_outer,
            text="🧮 Modo de Ejecución",
            bootstyle="secondary",
            padding=10,
        )
        modo_frame.grid(
            row=2, column=0, columnspan=3, sticky="ew", pady=(8, 10)
        )
        self.rb_excel = ttk.Radiobutton(
            modo_frame,
            text="Generar Excel 📊",
            variable=self.modo_var,
            value="solo_excel",
            bootstyle="warning",
            state="disabled",
        )
        self.rb_excel.pack(anchor="w", pady=2)
        self.rb_both = ttk.Radiobutton(
            modo_frame,
            text="Generar Excel + Subida a Base de Datos (ETL) 🚀",
            variable=self.modo_var,
            value="excel_y_bd",
            bootstyle="success",
            state="disabled",
        )
        self.rb_both.pack(anchor="w", pady=2)

        # Contenedor para botones de control
        control_frame = ttk.Frame(conf_outer)
        control_frame.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(4, 0))
        control_frame.columnconfigure((0, 1, 2), weight=1)

        self.btn_procesar = ttk.Button(
            control_frame,
            text="🚀 Procesar Facturas",
            bootstyle="success",
            command=self.iniciar_procesamiento,
            state="disabled",
        )
        self.btn_procesar.grid(row=0, column=0, sticky="ew", padx=2)

        self.btn_pausar = ttk.Button(
            control_frame,
            text="⏸️ Pausar",
            bootstyle="warning",
            command=self.pausar_procesamiento,
            state="disabled",
        )
        self.btn_pausar.grid(row=0, column=1, sticky="ew", padx=2)

        self.btn_cancelar = ttk.Button(
            control_frame,
            text="⏹️ Cancelar",
            bootstyle="danger",
            command=self.cancelar_procesamiento,
            state="disabled",
        )
        self.btn_cancelar.grid(row=0, column=2, sticky="ew", padx=2)

        self.progress = ttk.Progressbar(
            conf_outer, bootstyle="success-striped", mode="indeterminate"
        )
        self.progress.grid(
            row=4, column=0, columnspan=3, sticky="ew", pady=10
        )

        # ---------- Panel 1: Log ----------
        log_outer = ttk.Labelframe(
            pw, text="📋 Log del Proceso", padding=10, bootstyle="secondary"
        )
        log_outer.columnconfigure(0, weight=1)
        log_outer.rowconfigure(0, weight=1)

        log_scroll = ttk.Scrollbar(log_outer)
        log_scroll.grid(row=0, column=1, sticky="ns")

        self.log_text = Text(
            log_outer,
            height=12,
            wrap="word",
            bg="white",
            fg="black",
            yscrollcommand=log_scroll.set,
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll.config(command=self.log_text.yview)

        # ---------- Panel 2: Chat IA ----------
        chat_outer = ttk.Labelframe(
            pw,
            text="🤖 Asistente de Consultas Inteligentes",
            padding=10,
            bootstyle="info",
        )
        chat_outer.columnconfigure(0, weight=1)
        chat_outer.rowconfigure(0, weight=1)

        chat_scroll = ttk.Scrollbar(chat_outer)
        chat_scroll.grid(row=0, column=1, sticky="ns")

        self.chat_history = Text(
            chat_outer,
            height=10,
            wrap="word",
            bg="#f8f9fa",
            fg="black",
            font=("Arial", 10),
            yscrollcommand=chat_scroll.set,
            state="disabled",
        )
        self.chat_history.grid(
            row=0, column=0, sticky="nsew", pady=(0, 10)
        )
        chat_scroll.config(command=self.chat_history.yview)

        entry_frame = ttk.Frame(chat_outer)
        entry_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(10, 5))
        entry_frame.columnconfigure(1, weight=1) # El input de texto debe expandirse

        # Combo para seleccionar modelo
        self.ai_model_var = ttk.StringVar(value="Automático (Recomendado)")
        self.cb_ai_model = ttk.Combobox(
            entry_frame,
            textvariable=self.ai_model_var,
            values=["Automático (Recomendado)", "Solo Gemini (Nube)", "Solo Llama 3.1 (Local)"],
            state="readonly",
            width=22,
            font=("Arial", 10)
        )
        self.cb_ai_model.grid(row=0, column=0, sticky="w", padx=(0, 10))

        # Input de pregunta más grande y destacado
        self.chat_entry = ttk.Entry(
            entry_frame, font=("Arial", 12), state="disabled" 
        )
        self.chat_entry.grid(row=0, column=1, sticky="ew", padx=(0, 10), ipady=5)
        self.chat_entry.bind("<Return>", lambda e: self.enviar_pregunta())
        
        # Botón de enviar también un poco más destacado
        self.btn_enviar = ttk.Button(
            entry_frame,
            text="📤 Enviar",
            bootstyle="success",
            command=self.enviar_pregunta,
            width=14,
            state="disabled",
        )
        self.btn_enviar.grid(row=0, column=2, ipady=2)

        # Mensaje inicial neutral
        self._append_chat(
            "🤖 Asistente: Bienvenido. Una vez que inicies sesión y proceses tus facturas, "
            "podrás hacer consultas tributarias inteligentes."
        )

        # Añadir paneles y pesos
        pw.add(conf_outer)
        pw.add(log_outer)
        pw.add(chat_outer)
        try:
            pw.paneconfigure(conf_outer, weight=2)
            pw.paneconfigure(log_outer, weight=3)
            pw.paneconfigure(chat_outer, weight=3)
        except Exception:
            pass

        # Alturas iniciales agradables para que se vea el botón
        self._init_pane_sizes(pw)

    def _init_pane_sizes(self, pw: ttk.Panedwindow):
        """Coloca las divisiones para que se vea bien el botón y el chat."""
        def apply():
            try:
                h = pw.winfo_height()
                if h <= 1:
                    self.root.after(120, apply)
                    return
                # 40% Config, 30% Log, 30% Chat
                pw.sashpos(0, int(h * 0.40))
                pw.sashpos(1, int(h * 0.70))
            except Exception:
                pass

        self.root.after(150, apply)

    # ----------------------------
    # Procesamiento
    # ----------------------------
    def seleccionar_entrada(self):
        carpeta = filedialog.askdirectory()
        if carpeta:
            self.carpeta_entrada = Path(carpeta)
            valor = str(self.carpeta_entrada)
            self.entrada_var.set(valor)
            self.ent_entrada.configure(state="normal")
            self.ent_entrada.delete(0, END)
            self.ent_entrada.insert(0, valor)
            self._verificar_rutas()

    def seleccionar_salida(self):
        carpeta = filedialog.askdirectory()
        if carpeta:
            self.carpeta_salida = Path(carpeta)
            valor = str(self.carpeta_salida)
            self.salida_var.set(valor)
            self.ent_salida.configure(state="normal")
            self.ent_salida.delete(0, END)
            self.ent_salida.insert(0, valor)
            self._verificar_rutas()

    def _verificar_rutas(self):
        ok = bool(
            self.carpeta_entrada
            and self.carpeta_salida
            and self.logged_in
            and not self.processing
        )
        self.btn_procesar.configure(state=("normal" if ok else "disabled"))

    def iniciar_procesamiento(self):
        if self.processing:
            return

        if not self.logged_in:
            messagebox.showwarning("Sesión", "Primero debes iniciar sesión.")
            return

        if not (self.carpeta_entrada and self.carpeta_salida):
            messagebox.showwarning(
                "Rutas", "Selecciona carpeta de entrada y salida."
            )
            return

        self.processing = True
        self.btn_procesar.configure(state="disabled")
        self.btn_pausar.configure(state="normal", text="⏸️ Pausar")
        self.btn_cancelar.configure(state="normal")
        self.progress.start()
        self.log_text.delete(1.0, "end")

        self.processor = FacturaProcessor(
            self.carpeta_entrada,
            self.carpeta_salida,
            self.log,                 # callback thread-safe
            tenant_id=self.tenant_id,
            root_window=self.root,    # ⭐ Pasar referencia al root
        )

        t = threading.Thread(target=self._thread_procesar, daemon=True)
        t.start()

    def _thread_procesar(self):
        try:
            persist_db = self.modo_var.get() == "excel_y_bd"
            self.processor.ejecutar_proceso(persist_db=persist_db)
        except Exception as e:
            msg = str(e)
            if self.root.winfo_exists():
                self.root.after(0, lambda: messagebox.showerror("Error", msg))
        finally:
            if self.root.winfo_exists():
                self.root.after(0, self.finalizar_proceso)

    def finalizar_proceso(self):
        self.processing = False
        self.progress.stop()
        self.btn_pausar.configure(state="disabled", text="⏸️ Pausar")
        self.btn_cancelar.configure(state="disabled")
        self._verificar_rutas()

    def pausar_procesamiento(self):
        if not self.processor:
            return
            
        if self.processor.pause_event.is_set():
            # Estaba pausado, reanudar
            self.processor.pause_event.clear()
            self.btn_pausar.configure(text="⏸️ Pausar", bootstyle="warning")
            self.log("▶️ Reanudando proceso por el usuario...")
        else:
            # Estaba corriendo, pausar
            self.processor.pause_event.set()
            self.btn_pausar.configure(text="▶️ Reanudar", bootstyle="info")
            self.log("⏸️ Proceso pausado por el usuario...")

    def cancelar_procesamiento(self):
        if not self.processor:
            return
            
        # Preguntar confirmación opcional o simplemente cancelar
        self.processor.cancel_event.set()
        # Si estaba pausado, hay que soltarlo para que pueda evaluar la condición de cancel_event y salir
        if self.processor.pause_event.is_set():
            self.processor.pause_event.clear()
            
        self.log("⏹️ Instrucción de cancelación enviada, esperando que termine la operación actual...")
        self.btn_cancelar.configure(state="disabled")
        self.btn_pausar.configure(state="disabled")

    def log(self, mensaje: str):
        """Log thread-safe: puede ser llamado desde hilos secundarios."""
        def _append():
            # Si la ventana ya no existe, no hacer nada
            if not self.root.winfo_exists():
                return
            self.log_text.insert("end", mensaje + "\n")
            self.log_text.see("end")

        if self.root.winfo_exists():
            self.root.after(0, _append)

    # ----------------------------
    # Chat IA (CORREGIDO)
    # ----------------------------
    def enviar_pregunta(self):
        if not self.logged_in or not self.assistant:
            return

        pregunta = self.chat_entry.get().strip()
        if not pregunta:
            return

        self._append_chat(f"💤 Tú: {pregunta}")
        self.chat_entry.delete(0, "end")
        self._append_chat("🤖 Asistente: escribiendo...")

        # Deshabilitar entrada mientras el asistente responde
        self.chat_entry.configure(state="disabled")
        self.btn_enviar.configure(state="disabled")

        def _proc():
            try:
                selected_model_option = self.ai_model_var.get()
                respuesta = self.assistant.ask(pregunta, model_option=selected_model_option)
                # IMPORTANTE: Usar self.root.after() para actualizar UI
                self.root.after(0, lambda: self._finish_chat(respuesta))
            except Exception as e:
                msg_err = f"Error: {str(e)}"
                self.root.after(0, lambda: self._finish_chat(msg_err))

        threading.Thread(target=_proc, daemon=True).start()

    def _finish_chat(self, respuesta):
        """Reemplaza la línea 'escribiendo...', procesa texto u opciones ricas, y re-habilita el input."""
        self.chat_history.configure(state="normal")
        try:
            # Buscar y borrar "escribiendo..."
            content = self.chat_history.get("1.0", "end-1c")
            lines = content.split("\n")
            for i in range(len(lines) - 1, -1, -1):
                if "escribiendo..." in lines[i]:
                    self.chat_history.delete(f"{i+1}.0", "end")
                    break
        except Exception as e:
            print(f"Error borrando 'escribiendo...': {e}")
            
        # Insertar la nueva respuesta (dict o string)
        if isinstance(respuesta, dict):
            # Texto principal
            msg_text = respuesta.get('text', '')
            if not msg_text.startswith("🤖"):
                msg_text = f"🤖 Asistente: {msg_text}"
            self.chat_history.insert("end", msg_text + "\n\n")
            
            # Opciones como botones
            options = respuesta.get('options', [])
            if options:
                # Contenedor para alinear los botones
                btn_frame = ttk.Frame(self.chat_history)
                for opt in options:
                    btn = ttk.Button(
                        btn_frame,
                        text=opt['label'],
                        bootstyle="outline-info",
                        cursor="hand2",
                        command=lambda action=opt['action']: self._send_auto_query(action)
                    )
                    btn.pack(side="left", padx=2, pady=2)
                self.chat_history.window_create("end", window=btn_frame)
                self.chat_history.insert("end", "\n\n")
        else:
            msg_text = str(respuesta)
            if not msg_text.startswith("🤖") and not msg_text.startswith("📢") and not msg_text.startswith("📊") and not msg_text.startswith("🔎"):
                msg_text = f"🤖 Asistente: {msg_text}"
            self.chat_history.insert("end", msg_text + "\n\n")

        self.chat_history.see("end")
        self.chat_history.configure(state="disabled")
        
        self.chat_entry.configure(state="normal")
        self.btn_enviar.configure(state="normal")
        self.chat_entry.focus_set()

    def _send_auto_query(self, query: str):
        """Envía una pregunta automáticamente (ej. al hacer clic en un botón de sugerencia)."""
        self.chat_entry.configure(state="normal")
        self.chat_entry.delete(0, "end")
        self.chat_entry.insert(0, query)
        self.enviar_pregunta()

    def _append_chat(self, text: str):
        """DEBE ser llamado desde el hilo principal o vía root.after()"""
        self.chat_history.configure(state="normal")
        self.chat_history.insert("end", text + "\n\n")
        self.chat_history.see("end")
        self.chat_history.configure(state="disabled")

    # ----------------------------
    # Cierre
    # ----------------------------
    def _on_close(self):
        self.root.destroy()
