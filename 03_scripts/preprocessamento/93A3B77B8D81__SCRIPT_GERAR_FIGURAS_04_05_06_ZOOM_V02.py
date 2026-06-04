# -*- coding: utf-8 -*-
"""
SCRIPT V02 - FIGURAS 04, 05 E 06 COM ZOOM NOS PIXELS VALIDOS
Gera mapas tematicos reais a partir do GeoTIFF:
STEP06_HYPERION_ACRE_20120709_INDICES_CO2FLUX_v01.tif

Banda 1 = NDVI
Banda 2 = PRI
Banda 3 = sPRI
Banda 4 = CO2Flux
"""

from pathlib import Path
import numpy as np
import rasterio
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D
from matplotlib.colors import Normalize

BASE_DIR = Path(r"C:\Users\macie\Downloads\ARTIGO_CO2_ACRE_HYPERION")
RASTER_PATH = BASE_DIR / "STEP06_HYPERION_ACRE_20120709_INDICES_CO2FLUX_v01.tif"
OUT_DIR = BASE_DIR / "FIGURAS_GERADAS_PY_V02"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SPECS = {
    "PRI": {
        "band": 2,
        "title": "MAPA DO PRI DA AREA AGRICOLA PROCESSADA",
        "subtitle": "Indice de Reflectancia Fotoquimica em Outras Lavouras Temporarias",
        "outfile": "FIGURA_04_PRI_HYPERION_ACRE_ZOOM_V02.png",
        "cmap": "cool",
        "vmin": 0.017487047240138054,
        "vmax": 0.043849438428878784,
        "mean": 0.0327600758382172,
        "std": 0.00664862469268531,
        "label": "PRI"
    },
    "sPRI": {
        "band": 3,
        "title": "MAPA DO sPRI DA AREA AGRICOLA PROCESSADA",
        "subtitle": "PRI reescalonado em Outras Lavouras Temporarias",
        "outfile": "FIGURA_05_sPRI_HYPERION_ACRE_ZOOM_V02.png",
        "cmap": "plasma",
        "vmin": 0.508743523620069,
        "vmax": 0.5219247192144394,
        "mean": 0.5163800379191086,
        "std": 0.003324312346353195,
        "label": "sPRI"
    },
    "CO2Flux": {
        "band": 4,
        "title": "MAPA DO CO2FLUX DA AREA AGRICOLA PROCESSADA",
        "subtitle": "Fluxo estimado de CO2 em Outras Lavouras Temporarias",
        "outfile": "FIGURA_06_CO2FLUX_HYPERION_ACRE_ZOOM_V02.png",
        "cmap": "RdYlGn_r",
        "vmin": -13.258663322170465,
        "vmax": 1.2766830139090555,
        "mean": -10.458159729732714,
        "std": 4.521038268756586,
        "label": "CO2Flux"
    }
}

def read_band(src, idx):
    arr = src.read(idx).astype("float64")
    if src.nodata is not None:
        arr[arr == src.nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr

def add_north_arrow(ax):
    ax.annotate(
        "N",
        xy=(0.93, 0.90),
        xytext=(0.93, 0.78),
        xycoords="axes fraction",
        textcoords="axes fraction",
        arrowprops=dict(arrowstyle="-|>", lw=1.5, color="black"),
        ha="center",
        va="center",
        fontsize=12,
        fontweight="bold"
    )

def add_scale_bar(ax, length_m=300):
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    x0 = xlim[0] + (xlim[1] - xlim[0]) * 0.08
    y0 = ylim[0] + (ylim[1] - ylim[0]) * 0.07
    ax.plot([x0, x0 + length_m], [y0, y0], color="black", lw=4, solid_capstyle="butt")
    ax.text(x0, y0 + 18, "0", ha="center", va="bottom", fontsize=9)
    ax.text(x0 + length_m, y0 + 18, f"{length_m} m", ha="center", va="bottom", fontsize=9)

def draw_one(src, all_bands, mask, spec):
    arr = all_bands[spec["band"]].copy()
    arr[~mask] = np.nan

    rows, cols = np.where(mask)
    if len(rows) == 0:
        raise RuntimeError("Nenhum pixel valido encontrado.")

    pad = 4
    r0 = max(int(rows.min()) - pad, 0)
    r1 = min(int(rows.max()) + pad + 1, src.height)
    c0 = max(int(cols.min()) - pad, 0)
    c1 = min(int(cols.max()) + pad + 1, src.width)

    crop = arr[r0:r1, c0:c1]

    transform = src.transform
    px_w = transform.a
    px_h = abs(transform.e)

    xmin = transform.c + c0 * transform.a
    xmax = transform.c + c1 * transform.a
    ymax = transform.f + r0 * transform.e
    ymin = transform.f + r1 * transform.e

    fig = plt.figure(figsize=(12, 8), dpi=300)

    ax_header = fig.add_axes([0.04, 0.90, 0.92, 0.08])
    ax_header.set_axis_off()
    ax_header.add_patch(Rectangle((0, 0), 1, 1, transform=ax_header.transAxes, fill=False, edgecolor="gray", lw=1))
    ax_header.text(0.5, 0.68, spec["title"], ha="center", va="center", fontsize=18, fontweight="bold")
    ax_header.text(0.5, 0.30, spec["subtitle"], ha="center", va="center", fontsize=12)

    ax = fig.add_axes([0.06, 0.12, 0.68, 0.73])
    ax.set_facecolor("#f3f3f0")

    im = ax.imshow(
        crop,
        extent=(xmin, xmax, ymin, ymax),
        origin="upper",
        cmap=spec["cmap"],
        vmin=spec["vmin"],
        vmax=spec["vmax"],
        interpolation="nearest"
    )

    ax.set_title("Recorte ampliado dos pixels agricolas validos", fontsize=11, pad=8)
    ax.set_xlabel("Coordenada X UTM")
    ax.set_ylabel("Coordenada Y UTM")
    ax.grid(True, color="gray", alpha=0.35, linewidth=0.5)
    ax.set_aspect("equal", adjustable="box")

    for x in np.arange(xmin, xmax + px_w, px_w):
        ax.axvline(x, color="black", lw=0.15, alpha=0.35)
    for y in np.arange(ymin, ymax + px_h, px_h):
        ax.axhline(y, color="black", lw=0.15, alpha=0.35)

    add_north_arrow(ax)
    add_scale_bar(ax, 300)

    coord_text = "Sistema de Coordenadas: SIRGAS 2000\nProjecao: UTM Zona 19S\nDatum: SIRGAS 2000"
    ax.text(
        0.02, 0.02, coord_text,
        transform=ax.transAxes,
        fontsize=8,
        bbox=dict(boxstyle="round,pad=0.35", facecolor="white", edgecolor="gray")
    )

    ax_panel = fig.add_axes([0.77, 0.12, 0.19, 0.73])
    ax_panel.set_axis_off()
    ax_panel.add_patch(Rectangle((0, 0), 1, 1, transform=ax_panel.transAxes, fill=False, edgecolor="gray", lw=1))

    ax_panel.text(0.5, 0.94, "Legenda", ha="center", va="center", fontsize=12, fontweight="bold")
    ax_panel.scatter([0.12], [0.83], s=70, marker="s", color="gray")
    ax_panel.text(0.22, 0.83, "Pixel agricola valido", va="center", fontsize=9)

    cax = fig.add_axes([0.81, 0.42, 0.04, 0.28])
    cb = fig.colorbar(im, cax=cax)
    cb.set_label(spec["label"], fontsize=9)
    cb.ax.tick_params(labelsize=8)

    ax_panel.text(
        0.08, 0.32,
        "Estatisticas\n"
        f"Media: {spec['mean']:.6f}\n"
        f"Minimo: {np.nanmin(arr):.6f}\n"
        f"Maximo: {np.nanmax(arr):.6f}\n"
        f"Desvio-padrao: {spec['std']:.6f}\n"
        f"Pixels validos: {int(mask.sum())}",
        ha="left",
        va="top",
        fontsize=9
    )

    ax_panel.text(
        0.08, 0.09,
        "Elaboracao: autores.\n"
        "Dados: EO-1 Hyperion,\n"
        "MapBiomas 2012 e INMET A104.",
        ha="left",
        va="bottom",
        fontsize=8
    )

    out = OUT_DIR / spec["outfile"]
    fig.savefig(out, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"OK: {out}")

def main():
    if not RASTER_PATH.exists():
        raise FileNotFoundError(f"Raster nao encontrado: {RASTER_PATH}")

    with rasterio.open(RASTER_PATH) as src:
        bands = {i: read_band(src, i) for i in [1, 2, 3, 4]}

        ndvi = bands[1]
        pri = bands[2]
        spri = bands[3]
        co2 = bands[4]

        mask = (
            np.isfinite(ndvi) &
            np.isfinite(pri) &
            np.isfinite(spri) &
            np.isfinite(co2) &
            (ndvi >= 0.30) & (ndvi <= 1.00) &
            (pri >= 0.00) & (pri <= 0.08) &
            (spri >= 0.50) & (spri <= 0.53) &
            (co2 >= -20.00) & (co2 <= 5.00)
        )

        print(f"PIXELS_VALIDOS_DETECTADOS={int(mask.sum())}")

        for name, spec in SPECS.items():
            draw_one(src, bands, mask, spec)

    print(f"OK: FIGURAS_DIR={OUT_DIR}")

if __name__ == "__main__":
    main()
