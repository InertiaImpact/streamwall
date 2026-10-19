"""Regenerate the vector source and Windows icon (requires Pillow)."""
from pathlib import Path
from PIL import Image, ImageDraw

assets = Path(__file__).resolve().parent.parent / "assets"
assets.mkdir(exist_ok=True)
tiles = [(10, 10, "#38bdf8"), (34, 10, "#0284c7"),
         (10, 34, "#0284c7"), (34, 34, "#fbbf24")]
svg = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">',
       '  <rect width="64" height="64" rx="12" fill="#0f172a"/>']
for x, y, color in tiles:
    svg.append(f'  <rect x="{x}" y="{y}" width="20" height="20" rx="3" fill="{color}"/>')
svg.append('</svg>')
(assets / "streamwall.svg").write_text("\n".join(svg) + "\n", encoding="utf-8")

factor = 16
image = Image.new("RGBA", (64 * factor, 64 * factor))
draw = ImageDraw.Draw(image)
draw.rounded_rectangle((0, 0, 64 * factor - 1, 64 * factor - 1),
                       radius=12 * factor, fill="#0f172a")
for x, y, color in tiles:
    draw.rounded_rectangle((x * factor, y * factor, (x + 20) * factor - 1, (y + 20) * factor - 1),
                           radius=3 * factor, fill=color)
image = image.resize((256, 256), Image.Resampling.LANCZOS)
image.save(assets / "streamwall.ico", format="ICO",
           sizes=[(size, size) for size in (16, 20, 24, 32, 40, 48, 64, 128, 256)])
