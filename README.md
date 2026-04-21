# 📊 Gestión Tributaria

Sistema de escritorio asistido por IA para la gestión tributaria de MiPymes colombianas. Automatiza la extracción, validación y consulta de facturas electrónicas UBL 2.1 (XML/ZIP) con un asistente dual de inteligencia artificial (Ollama/Llama 3.1 + Gemini 2.5 Flash) y almacenamiento en PostgreSQL.

> **Proyecto de Grado** - Programa de Ingeniería de Sistemas  
> **Institución:** Universidad Santiago de Cali (USC) - 2026

---

## 📌 Resumen del Proyecto

Este software fue diseñado para resolver problemas de lentitud y errores humanos en la extracción masiva de datos fiscales (XML/Excel). Permite cargar carpetas enteras de archivos, procesarlos automáticamente y extraer la información tributaria y base contable para generar visualizaciones, análisis y hojas de reporte exportables.

Además, cuenta con un asistente interactivo alimentado por Inteligencia Artificial que ejecuta análisis semánticos empleando motores embebidos en el instalador, asegurando la máxima privacidad de los datos empresariales.

---

## 👥 Créditos y Autores

* **Desarrollador Principal:** Álvaro José Mosquera Morales
* **Director del Proyecto:** Jair Enrique Sanclemente Castro
* **Institución:** Universidad Santiago de Cali

---

## 🏗️ Arquitectura y Tecnologías Principales

El proyecto ha sido diseñado bajo arquitectura multicapa siguiendo las mejores prácticas de ingeniería de software.

- **Lenguaje Base:** Python 3.13+
- **Interfaz Gráfica (GUI):** `ttkbootstrap` orientada a componentes.
- **Inteligencia Artificial:** Gemini 2.5 Flash en nube y Ollama con su modelo Llama 3.1 (8B) de manera local.
- **Ecosistema de Dependencias:** Gestión ultrarrápida usando `uv`.
- **Empaquetado y Distribución:** Compilado binario con `PyInstaller` y empaquetado multiparte con `Inno Setup Compiler`.

---

## 🚀 Guía de Instalación

Existen dos maneras de disponer de la aplicación, ya sea para su uso productivo inmediato o para el desarrollo técnico.

### 1. Instrucciones para Usuarios Finales (Productivo)

Si deseas usar la aplicación sin entrar al código fuente, busca la carpeta generada (o descárgala del release oficial):

1. Copia a tu ordenador los archivos `GestionTributaria_Setup_v1.0.0.exe` junto a sus binarios dependientes (`.bin` files). *Todos los archivos deben estar en la misma carpeta*.
2. Haz doble clic en el instalador `.exe`. El asistente de "Inno Setup" ensamblará y compilará la base de datos y la IA en tu ordenador.
3. Se generará un acceso directo en el escritorio y podrás consultar facturas totalmente fuera de línea.

### 2. Instrucciones para Desarrolladores (Código Fuente)

Se recomienda encarecidamente el uso de [uv](https://github.com/astral-sh/uv) como gestor de paquetes para Python.

**A) Clonar e inicializar**

```bash
git clone https://github.com/AlvaroJMosquera/gestion-tributaria.git
cd gestion-tributaria
```

**B) Instalar dependencias mediante `uv`**

Genera o activa el entorno virtual e instala los requerimientos oficiales del proyecto (omitiendo paquetes masivos ajenos como torch).

```bash
# Windows
.venv\Scripts\activate

# Instalación hiperrápida:
uv pip install -r requirements.txt
```

**C) Configurar Entorno e Iniciar Aplicación**

Asegúrate de preparar y definir un archivo `.env` en la raíz si estás apuntando a bases de datos en la nube (como Supabase), o usa la conexión local.

```bash
python -m backend.app.presentation.main
```

---

## 📄 Licencia y Derechos de Uso

Este proyecto es propiedad intelectual académica. Su consulta está orientada al marco educativo y desarrollo universitario de la Universidad Santiago de Cali (USC). Para más detalles revisar el archivo `LICENSE`.
