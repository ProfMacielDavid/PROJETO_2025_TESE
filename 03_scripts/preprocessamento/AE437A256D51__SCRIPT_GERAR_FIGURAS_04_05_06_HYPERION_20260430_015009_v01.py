# -*- coding: utf-8 -*-
"""
SCRIPT PARA GERAR AS FIGURAS 4, 5 E 6
Artigo: Modelagem do Fluxo de CO2 em Cultivos de Soja na Amazônia Legal
Dados reais a partir do GeoTIFF:
STEP06_HYPERION_ACRE_20120709_INDICES_CO2FLUX_v01.tif

Compatível com VS Code (Windows) e também com Google Colab, com pequenos ajustes.
Autor: apoio ChatGPT
Versão: v01

FIGURAS GERADAS:
- Figura 4: PRI
- Figura 5: sPRI
- Figura 6: CO2Flux

MAPEAMENTO DAS BANDAS DO TIFF:
- Banda 1 = NDVI
- Banda 2 = PRI
- Banda 3 = sPRI
- Banda 4 = CO2Flux
"""

from pathlib import Path
import math
import numpy as np
import rasterio
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D

# =========================================================
# 1. CONFIGURAÇÕES
# =========================================================

# AJUSTE ESTE CAMINHO NO SEU COMPUTADOR
BASE_DIR = Path(r"C:\Users\macie\Downloads\ARTIGO_CO2_ACRE_HYPERION")

# GeoTIFF principal
RASTER_PATH = BASE_DIR / "STEP06_HYPERION_ACRE_20120709_INDICES_CO2FLUX_v01.tif"

# Pasta de saída
OUT_DIR = BASE_DIR / "FIGURAS_GERADAS_PY"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Coordenada de referência usada no mapa aprovado
# Observação: este ponto foi mantido dentro do footprint para coerência visual da série cartográfica.
# Se quiser a estação real de Rio Branco, substitua os valores abaixo.
REF_X = None
REF_Y = None

# Título geral do projeto
SUBTITLE_COMMON = "Análise hiperespectral em Outras Lavouras Temporárias"

# =========================================================
# 2. PARÂMETROS DAS FIGURAS
# =========================================================

FIG_SPECS = {
    "PRI": {
        "band": 2,
        "title": "MAPA DO PRI DA ÁREA AGRÍCOLA PROCESSADA",
        "subtitle": "Índice de Reflectância Fotoquímica em Outras Lavouras Temporárias",
        "cmap": "cool",
        "vmin": 0.017487047240138054,
        "vmax": 0.043849438428878784,
        "mean": 0.0327600758382172,
        "std": 0.00664862469268531,
        "min": 0.017487047240138054,
        "max": 0.043849438428878784,
        "outfile": "FIGURA_04_PRI_HYPERION_ACRE_PY.png",
        "legend_label": "PRI",
    },
    "sPRI": {
        "band": 3,
        "title": "MAPA DO sPRI DA ÁREA AGRÍCOLA PROCESSADA",
        "subtitle": "Índice PRI Escalonado em Outras Lavouras Temporárias",
        "cmap": "plasma",
        "vmin": 0.508743523620069,
        "vmax": 0.5219247192144394,
        "mean": 0.5163800379191086,
        "std": 0.003324312346353195,
        "min": 0.508743523620069,
        "max": 0.5219247192144394,
        "outfile": "FIGURA_05_sPRI_HYPERION_ACRE_PY.png",
        "legend_label": "sPRI",
    },
    "CO2Flux": {
        "band": 4,
        "title": "MAPA DO CO2FLUX DA ÁREA AGRÍCOLA PROCESSADA",
        "subtitle": "Fluxo estimado de CO2 em Outras Lavouras Temporárias",
        "cmap": "RdYlGn_r",
        "vmin": -13.258663322170465,
        "vmax": 1.2766830139090555,
        "mean": -10.458159729732714,
        "std": 4.521038268756586,
        "min": -13.258663322170465,
        "max": 1.2766830139090555,
        "outfile": "FIGURA_06_CO2FLUX_HYPERION_ACRE_PY.png",
        "legend_label": "CO2Flux",
    },
}

# =========================================================
# 3. FUNÇÕES AUXILIARES
# =========================================================

def read_band(src, band_index):
    arr = src.read(band_index).astype(float)
    nodata = src.nodata
    if nodata is not None:
        arr[arr == nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr

def get_valid_points(src, mask_arr):
    rows, cols = np.where(np.isfinite(mask_arr))
    xs, ys = rasterio.transform.xy(src.transform, rows, cols, offset="center")
    return np.array(xs), np.array(ys)

def add_scale_bar(ax, x0, y0, length, label):
    ax.plot([x0, x0 + length], [y0, y0], color="black", linewidth=3, solid_capstyle="butt")
    ax.plot([x0, x0 + length / 2], [y0, y0], color="black", linewidth=8, solid_capstyle="butt")
    ax.plot([x0 + length / 2, x0 + length], [y0, y0], color="white", linewidth=8, solid_capstyle="butt")
    ax.plot([x0, x0 + length], [y0, y0], color="black", linewidth=1.2)
    ax.text(x0, y0 + length * 0.05, "0", fontsize=9, ha="center")
    ax.text(x0 + length / 2, y0 + length * 0.05, label.split("–")[0].strip(), fontsize=9, ha="center")
    ax.text(x0 + length, y0 + length * 0.05, label.split("–")[-1].strip(), fontsize=9, ha="center")

def add_north_arrow(ax, x, y, size):
    ax.annotate(
        "N",
        xy=(x, y + size),
        xytext=(x, y),
        arrowprops=dict(arrowstyle="-|>", lw=1.2, color="black"),
        ha="center",
        va="bottom",
        fontsize=12,
        fontweight="bold",
    )

def draw_figure(spec_name, spec, raster_path, out_dir):
    with rasterio.open(raster_path) as src:
        arr = read_band(src, spec["band"])
        ndvi_arr = read_band(src, 1)
        bounds = src.bounds
        crs = str(src.crs)

        xs, ys = get_valid_points(src, ndvi_arr)
        if xs.size == 0:
            raise RuntimeError("Nenhum pixel válido foi encontrado no raster.")

        x_min = float(np.min(xs))
        x_max = float(np.max(xs))
        y_min = float(np.min(ys))
        y_max = float(np.max(ys))

        # Célula aproximada
        px_w = abs(src.transform.a)
        px_h = abs(src.transform.e)

        # Define ponto de referência
        if REF_X is None or REF_Y is None:
            ref_x = float(np.mean(xs))
            ref_y = float(np.mean(ys))
        else:
            ref_x = REF_X
            ref_y = REF_Y

        # Margens para o layout
        dx = x_max - x_min
        dy = y_max - y_min
        if dx == 0:
            dx = px_w * 20
        if dy == 0:
            dy = px_h * 20

        pad_x = dx * 1.5
        pad_y = dy * 1.0

        map_xmin = x_min - pad_x
        map_xmax = x_max + pad_x
        map_ymin = y_min - pad_y
        map_ymax = y_max + pad_y

        # Figura
        fig = plt.figure(figsize=(14, 10), dpi=300)

        # Cabeçalho
        ax_header = fig.add_axes([0.02, 0.91, 0.96, 0.08])
        ax_header.set_axis_off()
        ax_header.add_patch(Rectangle((0, 0), 1, 1, fill=False, edgecolor="gray", linewidth=1))
        ax_header.text(0.5, 0.72, spec["title"], ha="center", va="center", fontsize=22, fontweight="bold")
        ax_header.text(0.5, 0.32, spec["subtitle"], ha="center", va="center", fontsize=15)

        # Mapa principal
        ax = fig.add_axes([0.04, 0.08, 0.74, 0.80])
        ax.set_xlim(map_xmin, map_xmax)
        ax.set_ylim(map_ymin, map_ymax)

        # Fundo claro
        ax.set_facecolor("#eef1e8")

        # Grid leve
        ax.grid(True, linewidth=0.4, color="gray", alpha=0.35)

        # Footprint do raster
        ax.add_patch(
            Rectangle(
                (bounds.left, bounds.bottom),
                bounds.right - bounds.left,
                bounds.top - bounds.bottom,
                fill=False,
                edgecolor="blue",
                linewidth=2.0,
                zorder=3
            )
        )

        # Pixels válidos como células
        valid_rows, valid_cols = np.where(np.isfinite(arr))
        valid_vals = arr[np.isfinite(arr)]

        for row, col, val in zip(valid_rows, valid_cols, valid_vals):
            x_left = src.transform.c + col * src.transform.a
            y_top = src.transform.f + row * src.transform.e
            if src.transform.e < 0:
                y_bottom = y_top + src.transform.e
            else:
                y_bottom = y_top
            rect = Rectangle(
                (x_left, y_bottom),
                px_w,
                px_h,
                facecolor=plt.get_cmap(spec["cmap"])(
                    (val - spec["vmin"]) / (spec["vmax"] - spec["vmin"]) if spec["vmax"] != spec["vmin"] else 0.5
                ),
                edgecolor="black",
                linewidth=0.15,
                zorder=4
            )
            ax.add_patch(rect)

        # Ponto de referência
        ax.scatter(ref_x, ref_y, s=70, color="red", edgecolor="black", zorder=5)

        # Textos regionais simples
        ax.text(map_xmin + (map_xmax - map_xmin) * 0.10, map_ymin + (map_ymax - map_ymin) * 0.55,
                "ACRE", fontsize=18, color="darkgreen", fontweight="bold", alpha=0.9)
        ax.text(map_xmin + (map_xmax - map_xmin) * 0.40, map_ymin + (map_ymax - map_ymin) * 0.87,
                "AMAZONAS", fontsize=18, color="#6b5a2b", fontweight="bold", alpha=0.9)

        # Caixa de coordenadas
        coord_text = "Sistema de Coordenadas: SIRGAS 2000\nProjeção: UTM Zona 19S\nDatum: SIRGAS 2000"
        ax.text(
            0.015, 0.02, coord_text,
            transform=ax.transAxes, fontsize=9,
            bbox=dict(boxstyle="round,pad=0.35", facecolor="white", edgecolor="gray")
        )

        # Remover ticks longos
        ax.tick_params(labelsize=8)

        # Painel direito - localização
        ax_loc = fig.add_axes([0.80, 0.56, 0.18, 0.30])
        ax_loc.set_axis_off()
        ax_loc.add_patch(Rectangle((0, 0), 1, 1, fill=False, edgecolor="gray", linewidth=1))
        ax_loc.text(0.5, 0.93, "Mapa de Localização", ha="center", va="center", fontsize=12)
        # Brasil simplificado esquemático
        ax_loc.add_patch(Rectangle((0.12, 0.12), 0.76, 0.70, fill=True, facecolor="#d9e8f6", edgecolor="gray"))
        ax_loc.add_patch(Rectangle((0.30, 0.22), 0.42, 0.45, fill=True, facecolor="#efe9dc", edgecolor="gray"))
        ax_loc.add_patch(Rectangle((0.33, 0.44), 0.10, 0.05, fill=True, facecolor="red", edgecolor="darkred"))

        # Painel legenda
        ax_leg = fig.add_axes([0.80, 0.34, 0.18, 0.20])
        ax_leg.set_axis_off()
        ax_leg.add_patch(Rectangle((0, 0), 1, 1, fill=False, edgecolor="gray", linewidth=1))
        ax_leg.text(0.5, 0.87, "Legenda", ha="center", va="center", fontsize=12, fontweight="bold")

        ax_leg.add_line(Line2D([0.08, 0.20], [0.70, 0.70], color="blue", lw=2))
        ax_leg.text(0.26, 0.70, "Footprint EO-1 Hyperion", va="center", fontsize=9)

        ax_leg.scatter([0.14], [0.54], s=50, color="red", edgecolors="black")
        ax_leg.text(0.26, 0.54, "Ponto de referência", va="center", fontsize=9)

        # Barra de cores manual
        grad = np.linspace(spec["vmin"], spec["vmax"], 256).reshape(1, 256)
        ax_cbar = fig.add_axes([0.825, 0.385, 0.012, 0.095])
        ax_cbar.imshow(grad.T, aspect="auto", cmap=spec["cmap"], origin="lower")
        ax_cbar.set_xticks([])
        ax_cbar.set_yticks([0, 128, 255])
        ax_cbar.set_yticklabels([
            f"{spec['min']:.3f}",
            f"{spec['mean']:.3f}",
            f"{spec['max']:.3f}"
        ], fontsize=7)

        ax_leg.text(0.78, 0.70, spec["legend_label"], fontsize=9, rotation=90, va="center")

        # Painel norte/escala
        ax_ns = fig.add_axes([0.80, 0.18, 0.18, 0.14])
        ax_ns.set_axis_off()
        ax_ns.add_patch(Rectangle((0, 0), 1, 1, fill=False, edgecolor="gray", linewidth=1))
        add_north_arrow(ax_ns, 0.5, 0.45, 0.22)
        ax_ns.text(0.50, 0.08, "0      50      100 km", ha="center", va="center", fontsize=9)

        # Painel notas
        ax_note = fig.add_axes([0.80, 0.05, 0.18, 0.11])
        ax_note.set_axis_off()
        ax_note.add_patch(Rectangle((0, 0), 1, 1, fill=False, edgecolor="gray", linewidth=1))
        note = (
            "Elaboração: autores.\n"
            "Dados de referência: EO-1 Hyperion, MapBiomas 2012 e INMET A104.\n"
            f"{spec['legend_label']} médio: {spec['mean']:.6f}.\n"
            f"Intervalo: {spec['min']:.6f} a {spec['max']:.6f}.\n"
            f"{len(valid_vals)} pixels válidos."
        )
        ax_note.text(0.04, 0.88, note, ha="left", va="top", fontsize=8.5)

        out_path = out_dir / spec["outfile"]
        fig.savefig(out_path, dpi=300, bbox_inches="tight")
        plt.close(fig)
        print(f"OK: FIGURA_GERADA={out_path}")

def main():
    if not RASTER_PATH.exists():
        raise FileNotFoundError(f"Raster não encontrado: {RASTER_PATH}")

    print("INICIO: geração das Figuras 4, 5 e 6")
    for spec_name, spec in FIG_SPECS.items():
        draw_figure(spec_name, spec, RASTER_PATH, OUT_DIR)
    print("FIM: figuras geradas com sucesso")

if __name__ == "__main__":
    main()
