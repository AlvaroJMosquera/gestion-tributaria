"""
Genera icon.ico para el instalador de Gestión Tributaria USC
Ejecutar: python generate_icon.py
Requiere: pip install Pillow
"""
from PIL import Image, ImageDraw, ImageFont
import os

def crear_icono():
    sizes = [256, 128, 64, 48, 32, 16]
    imagenes = []

    for size in sizes:
        img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        # Fondo circular azul oscuro USC
        margin = int(size * 0.04)
        draw.ellipse(
            [margin, margin, size - margin, size - margin],
            fill=(0, 70, 127, 255)  # Azul USC
        )

        # Letra "GT" centrada en blanco
        font_size = int(size * 0.38)
        try:
            font = ImageFont.truetype("arial.ttf", font_size)
        except:
            font = ImageFont.load_default()

        texto = "GT"
        bbox = draw.textbbox((0, 0), texto, font=font)
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]
        x = (size - w) // 2
        y = (size - h) // 2 - int(size * 0.02)
        draw.text((x, y), texto, fill=(255, 255, 255, 255), font=font)

        # Línea decorativa inferior
        line_y = int(size * 0.78)
        lw = int(size * 0.5)
        lx = (size - lw) // 2
        line_width = max(1, int(size * 0.04))
        draw.rectangle([lx, line_y, lx + lw, line_y + line_width], fill=(255, 200, 0, 255))  # Dorado

        imagenes.append(img)

    # Guardar como .ico con todos los tamaños
    output_path = os.path.join(os.path.dirname(__file__), 'icon.ico')
    imagenes[0].save(
        output_path,
        format='ICO',
        sizes=[(s, s) for s in sizes],
        append_images=imagenes[1:]
    )
    print(f"✅ Ícono generado: {output_path}")

if __name__ == '__main__':
    crear_icono()
