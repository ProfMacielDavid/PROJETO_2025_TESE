# SCRIPT PARA REPRODUZIR MAPA EM PYTHON
# Figura 2 – MAPA DA CLASSE AGRÍCOLA PROCESSADA
# Compatível com Google Colab ou VS Code
#
# Observação importante:
# O mapa anterior aceito na conversa foi produzido por geração de imagem, não por script Python.
# Portanto, este código não é o "mesmo script" usado na geração por IA.
# Este é um script novo, feito para reproduzir um mapa muito semelhante,
# com layout acadêmico, buffer, footprint Hyperion, ponto de referência,
# legenda, seta norte, escala e mapa de localização.
#
# Para uso no Google Colab, execute primeiro:
# !pip install geopandas rasterio shapely pyproj contextily matplotlib
#
# Para uso no VS Code, instale:
# pip install geopandas rasterio shapely pyproj contextily matplotlib

from pathlib import Path
import math
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D
import geopandas as gpd
import rasterio
from rasterio.warp import transform_bounds
from shapely.geometry import Point, box
import contextily as cx

# ============================================================
# 1. CONFIGURAÇÕES PRINCIPAIS
# ============================================================

# Ajuste a pasta conforme o seu ambiente
# No Colab, por exemplo:
# base = Path("/content")
# No VS Code local:
base = Path(r"C:\Users\macie\Downloads\ARTIGO_CO2_ACRE_HYPERION")

# Arquivo raster usado para obter o footprint Hyperion
raster_path = base / "STEP06_HYPERION_ACRE_20120709_INDICES_CO2FLUX_v01.tif"

# Saída
output_png = base / "FIGURA_02_MAPA_CLASSE_PROCESSADA.png"

# Escolha do ponto vermelho:
# "dentro_footprint" = coloca o ponto no centróide dos pixels válidos
# "estacao_real" = usa a coordenada real da estação INMET A104 em Rio Branco
modo_ponto = "dentro_footprint"

# Coordenada real da estação INMET A104 (Rio Branco)
estacao_real_lon = -68.165
estacao_real_lat = -9.95777777

# Buffer do estudo em km
buffer_km = 120

# ============================================================
# 2. LEITURA DO RASTER E GEOMETRIAS PRINCIPAIS
# ============================================================

with rasterio.open(raster_path) as src:
    band1 = src.read(1).astype(float)
    nodata = src.nodata
    crs_raster = src.crs
    bounds_raster = src.bounds

    if nodata is not None:
        band1[band1 == nodata] = np.nan
    band1[~np.isfinite(band1)] = np.nan

    # Máscara de pixels válidos
    valid = np.isfinite(band1)

    # Transformação índice -> coordenadas
    rows, cols = np.where(valid)
    xs, ys = rasterio.transform.xy(src.transform, rows, cols, offset="center")

# Bounding box do footprint inteiro da cena processada
footprint_raster = box(bounds_raster.left, bounds_raster.bottom, bounds_raster.right, bounds_raster.top)
gdf_footprint = gpd.GeoDataFrame({"name": ["Footprint EO-1 Hyperion"]}, geometry=[footprint_raster], crs=crs_raster)

# Pixels válidos
pixel_points = gpd.GeoDataFrame(
    {"classe": ["41"] * len(xs)},
    geometry=gpd.points_from_xy(xs, ys),
    crs=crs_raster
)

# Coordenada do ponto vermelho
if modo_ponto == "dentro_footprint":
    ponto_geom = pixel_points.unary_union.centroid
    nome_ponto = "Ponto de referência"
else:
    gdf_est = gpd.GeoDataFrame(
        {"name": ["Estação INMET A104"]},
        geometry=gpd.points_from_xy([estacao_real_lon], [estacao_real_lat]),
        crs="EPSG:4326"
    ).to_crs(crs_raster)
    ponto_geom = gdf_est.geometry.iloc[0]
    nome_ponto = "Estação INMET A104"

gdf_ponto = gpd.GeoDataFrame({"name": [nome_ponto]}, geometry=[ponto_geom], crs=crs_raster)

# Buffer de 120 km em torno do ponto
buffer_m = buffer_km * 1000.0
gdf_buffer = gpd.GeoDataFrame({"name": [f"Área de estudo (buffer {buffer_km} km)"]},
                              geometry=[ponto_geom.buffer(buffer_m)], crs=crs_raster)

# ============================================================
# 3. REPROJEÇÃO PARA WEB MERCATOR NO MAPA PRINCIPAL
# ============================================================

crs_plot = "EPSG:3857"
gdf_footprint_3857 = gdf_footprint.to_crs(crs_plot)
pixel_points_3857 = pixel_points.to_crs(crs_plot)
gdf_ponto_3857 = gdf_ponto.to_crs(crs_plot)
gdf_buffer_3857 = gdf_buffer.to_crs(crs_plot)

# Extensão do mapa principal
minx, miny, maxx, maxy = gdf_buffer_3857.total_bounds
padx = (maxx - minx) * 0.05
pady = (maxy - miny) * 0.05
minx -= padx
maxx += padx
miny -= pady
maxy += pady

# ============================================================
# 4. MAPA DE LOCALIZAÇÃO (INSET DO BRASIL)
# ============================================================

world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
brazil = world[world["name"] == "Brazil"].copy()

# Retângulo aproximado do Acre para destaque no mapa do Brasil
acre_box = box(-73.99, -11.20, -66.50, -7.10)
acre_gdf = gpd.GeoDataFrame({"name": ["Acre"]}, geometry=[acre_box], crs="EPSG:4326")

# ============================================================
# 5. FUNÇÕES AUXILIARES
# ============================================================

def add_north_arrow(ax, x=0.5, y=0.10, size=0.08):
    ax.annotate(
        "N",
        xy=(x, y + size),
        xytext=(x, y),
        xycoords="axes fraction",
        textcoords="axes fraction",
        arrowprops=dict(facecolor="black", width=3, headwidth=12),
        ha="center",
        va="center",
        fontsize=16,
        fontweight="bold"
    )

def add_scale_bar(ax, length_km=200, location=(0.12, 0.14), linewidth=6):
    # escala aproximada a partir do eixo em metros (EPSG:3857)
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    x0 = xlim[0] + (xlim[1] - xlim[0]) * location[0]
    y0 = ylim[0] + (ylim[1] - ylim[0]) * location[1]
    length_m = length_km * 1000.0

    seg = length_m / 4.0
    for i in range(4):
        xi = x0 + i * seg
        xf = xi + seg
        color = "black" if i % 2 == 0 else "white"
        ax.plot([xi, xf], [y0, y0], color="black", linewidth=linewidth, solid_capstyle="butt")
        ax.plot([xi, xf], [y0, y0], color=color, linewidth=linewidth - 2, solid_capstyle="butt")

    ax.text(x0, y0 + length_m * 0.02, "0", fontsize=10, ha="center")
    ax.text(x0 + seg, y0 + length_m * 0.02, "50", fontsize=10, ha="center")
    ax.text(x0 + 2 * seg, y0 + length_m * 0.02, "100", fontsize=10, ha="center")
    ax.text(x0 + 4 * seg, y0 + length_m * 0.02, f"{length_km} km", fontsize=10, ha="center")

# ============================================================
# 6. LAYOUT DA FIGURA
# ============================================================

fig = plt.figure(figsize=(14, 10), dpi=300)
gs = fig.add_gridspec(
    nrows=20, ncols=20,
    left=0.03, right=0.98, top=0.97, bottom=0.04,
    wspace=0.2, hspace=0.25
)

# Cabeçalho
ax_header = fig.add_subplot(gs[0:2, 0:20])
ax_header.axis("off")
ax_header.add_patch(Rectangle((0, 0), 1, 1, transform=ax_header.transAxes, fill=False, edgecolor="gray", linewidth=1))
ax_header.text(0.5, 0.72, "MAPA DA CLASSE AGRÍCOLA PROCESSADA", ha="center", va="center",
               fontsize=24, fontweight="bold")
ax_header.text(0.5, 0.32, "Outras Lavouras Temporárias, classe 41 do MapBiomas 2012",
               ha="center", va="center", fontsize=18)

# Mapa principal
ax_map = fig.add_subplot(gs[2:19, 0:16])
ax_map.set_xlim(minx, maxx)
ax_map.set_ylim(miny, maxy)

# Basemap
try:
    cx.add_basemap(ax_map, source=cx.providers.Esri.WorldTopoMap, crs=crs_plot, attribution=False)
except Exception:
    pass

# Camadas
gdf_buffer_3857.boundary.plot(ax=ax_map, color="gold", linewidth=1.8, alpha=0.9, zorder=3)
gdf_footprint_3857.boundary.plot(ax=ax_map, color="blue", linewidth=2.0, zorder=4)
pixel_points_3857.plot(ax=ax_map, color="magenta", markersize=8, alpha=0.85, zorder=5)
gdf_ponto_3857.plot(ax=ax_map, color="red", markersize=60, edgecolor="black", zorder=6)

# Rótulos principais
ax_map.text(0.24, 0.53, "ACRE", transform=ax_map.transAxes, fontsize=20, fontweight="bold",
            color="darkgreen", alpha=0.9)
ax_map.text(0.45, 0.86, "AMAZONAS", transform=ax_map.transAxes, fontsize=18, fontweight="bold",
            color="#6b5a2b", alpha=0.9)
ax_map.text(0.72, 0.07, "RONDÔNIA", transform=ax_map.transAxes, fontsize=18, color="#6b5a2b", alpha=0.85)
ax_map.text(0.03, 0.22, "PERU", transform=ax_map.transAxes, fontsize=16, color="gray")
ax_map.text(0.51, 0.02, "BOLÍVIA", transform=ax_map.transAxes, fontsize=16, color="gray")

ax_map.grid(True, color="gray", alpha=0.35, linewidth=0.6)
ax_map.set_xticks([])
ax_map.set_yticks([])
for spine in ax_map.spines.values():
    spine.set_edgecolor("gray")
    spine.set_linewidth(1)

# Caixa do sistema de coordenadas
coord_text = "Sistema de Coordenadas: SIRGAS 2000\nProjeção: UTM Zona 19S\nDatum: SIRGAS 2000"
ax_map.text(0.015, 0.02, coord_text, transform=ax_map.transAxes, fontsize=10,
            bbox=dict(boxstyle="round,pad=0.35", facecolor="white", edgecolor="gray"))

# Painel direito - mapa do Brasil
ax_inset = fig.add_subplot(gs[2:9, 16:20])
ax_inset.set_title("Mapa de Localização", fontsize=12, pad=8)
brazil.plot(ax=ax_inset, color="#efe9dc", edgecolor="gray", linewidth=0.8)
acre_gdf.plot(ax=ax_inset, color="red", edgecolor="darkred", linewidth=0.8)
ax_inset.set_xlim(-74, -30)
ax_inset.set_ylim(-34, 6)
ax_inset.set_aspect("equal")
ax_inset.set_xticks([])
ax_inset.set_yticks([])
for spine in ax_inset.spines.values():
    spine.set_edgecolor("gray")
    spine.set_linewidth(1)

# Painel da legenda
ax_leg = fig.add_subplot(gs[9:14, 16:20])
ax_leg.axis("off")
ax_leg.add_patch(Rectangle((0, 0), 1, 1, transform=ax_leg.transAxes, fill=False, edgecolor="gray", linewidth=1))
ax_leg.text(0.5, 0.90, "Legenda", ha="center", va="center", fontsize=14, fontweight="bold")

legend_elements = [
    Line2D([0], [0], color="gold", lw=2, label=f"Área de estudo (buffer {buffer_km} km)"),
    Line2D([0], [0], color="blue", lw=2, label="Footprint EO-1 Hyperion"),
    Line2D([0], [0], marker="s", color="magenta", lw=0, markersize=8, label="Classe 41 - Outras Lavouras Temporárias"),
    Line2D([0], [0], marker="o", color="red", markeredgecolor="black", lw=0, markersize=9, label=nome_ponto),
]
ax_leg.legend(handles=legend_elements, loc="center left", frameon=False, fontsize=10)

# Painel norte + escala
ax_ns = fig.add_subplot(gs[14:17, 16:20])
ax_ns.set_xticks([])
ax_ns.set_yticks([])
for spine in ax_ns.spines.values():
    spine.set_edgecolor("gray")
    spine.set_linewidth(1)
add_north_arrow(ax_ns, x=0.5, y=0.38, size=0.25)
ax_ns2 = ax_map  # usa o mapa principal para desenhar a barra de escala
add_scale_bar(ax_map, length_km=200)

# Painel de notas
ax_note = fig.add_subplot(gs[17:19, 16:20])
ax_note.axis("off")
ax_note.add_patch(Rectangle((0, 0), 1, 1, transform=ax_note.transAxes, fill=False, edgecolor="gray", linewidth=1))

area_ha = len(pixel_points) * 30 * 30 / 10000.0
note = (
    "Elaboração: autores.\n"
    "Dados de referência: EO-1 Hyperion, MapBiomas 2012 e INMET A104.\n"
    "Classe processada: 41, Outras Lavouras Temporárias.\n"
    f"Área válida aproximada: {area_ha:.6f} ha."
)
ax_note.text(0.05, 0.72, note, ha="left", va="top", fontsize=10)

plt.savefig(output_png, dpi=300, bbox_inches="tight")
plt.close()

print("OK: MAPA_GERADO")
print(f"OK: SAIDA={output_png}")
