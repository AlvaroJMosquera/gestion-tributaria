# Extraer Facturas

Sistema para la extracción, procesamiento y gestión de archivos de facturación.

## Tecnologías y Entorno de Desarrollo

Este proyecto recomienda el uso de [uv](https://github.com/astral-sh/uv) como gestor de paquetes ultrarrápido para Python, trabajando en conjunto con un entorno virtual (`.venv`).

### Instrucciones de Configuración

**1. Instalar dependencias usando `uv`**

Asegúrate de tener tu entorno virtual activo:
```bash
# En Windows:
.venv\Scripts\activate
```

Luego, instala las dependencias desde `requirements.txt`:
```bash
uv pip install -r requirements.txt
```
*(Si no tienes `uv` instalado, el comando equivalente tradicional es `pip install -r requirements.txt`).*

**2. Ejecutar la Aplicación**
```bash
python main.py
```
