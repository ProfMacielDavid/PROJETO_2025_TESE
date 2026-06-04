#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Capítulo 5 — Item xiv
Validação estrutural e estatística da tabela meteorológica em GPU (cuDF).

Objetivo:
- Carregar dataset meteorológico em GPU a partir de caminho canônico.
- Executar validações estruturais (schema, tipos, nulos, duplicatas).
- Executar validações estatísticas (descritivas numéricas, ranges, quantis).
- Gerar artefatos (logs, tabelas, figuras, metadata) para tese e Git.

ATENÇÃO:
- Este script não deve ser modificado manualmente para caminhos.
- Caminhos ficam em: configs/cap5_paths.env
"""

from __future__ import annotations

import sys
import json
import time
import hashlib
import platform
import subprocess
from pathlib import Path
from datetime import datetime

import numpy as np

import cudf
import cupy as cp

import matplotlib.pyplot as plt


def now_utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_env_file(env_path: Path) -> dict:
    if not env_path.exists():
        raise FileNotFoundError(f"Arquivo .env não encontrado: {env_path}")
    out = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def run_cmd(cmd: list[str]) -> str:
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        s = (res.stdout or "") + (res.stderr or "")
        return s.strip()
    except Exception as e:
        return f"[erro ao executar comando {cmd}]: {e}"


def safe_to_pandas_sample(df: cudf.DataFrame, n: int = 200_000, random_state: int = 42):
    n = int(n)
    if len(df) <= n:
        return df.to_pandas()
    frac = n / max(len(df), 1)
    return df.sample(frac=frac, random_state=random_state).to_pandas()


def is_numeric_dtype(dtype) -> bool:
    s = str(dtype)
    return any(t in s for t in ["int", "float", "uint", "decimal"])


def schema_table(df: cudf.DataFrame) -> cudf.DataFrame:
    return cudf.DataFrame(
        {
            "coluna": df.columns,
            "dtype": [str(df[c].dtype) for c in df.columns],
            "nulos": [int(df[c].isna().sum()) for c in df.columns],
        }
    )


def basic_profile(df: cudf.DataFrame) -> dict:
    return {
        "n_linhas": int(len(df)),
        "n_colunas": int(df.shape[1]),
        "memoria_gpu_bytes_est": int(df.memory_usage(deep=True).sum()),
    }


def duplicates_info(df: cudf.DataFrame) -> dict:
    t0 = time.time()
    dup_rows = int(df.duplicated().sum())
    dt = time.time() - t0
    return {"linhas_duplicadas": dup_rows, "tempo_s": round(dt, 3)}


def numeric_describe(df: cudf.DataFrame) -> cudf.DataFrame:
    num_cols = [c for c in df.columns if is_numeric_dtype(df[c].dtype)]
    if not num_cols:
        return cudf.DataFrame()
    desc = df[num_cols].describe()
    return desc.reset_index().rename(columns={"index": "estatistica"})


def numeric_quantiles(df: cudf.DataFrame, qs=(0.01, 0.05, 0.95, 0.99)) -> cudf.DataFrame:
    num_cols = [c for c in df.columns if is_numeric_dtype(df[c].dtype)]
    if not num_cols:
        return cudf.DataFrame()
    qdf = df[num_cols].quantile(list(qs), interpolation="linear")
    qdf = qdf.reset_index().rename(columns={"index": "quantil"})
    qdf["quantil"] = qdf["quantil"].astype(str)
    return qdf


def range_flags(df: cudf.DataFrame) -> cudf.DataFrame:
    num_cols = [c for c in df.columns if is_numeric_dtype(df[c].dtype)]
    if not num_cols:
        return cudf.DataFrame()
    mins = df[num_cols].min()
    maxs = df[num_cols].max()
    out = cudf.DataFrame({"coluna": num_cols, "min": mins.values, "max": maxs.values})
    out["range"] = out["max"] - out["min"]
    out["flag_invertido"] = (out["max"] < out["min"])
    try:
        thr = float(out["range"].quantile(0.95))
        out["flag_range_muito_alto"] = out["range"] > thr
    except Exception:
        out["flag_range_muito_alto"] = False
    return out


def save_figures(df: cudf.DataFrame, out_fig: Path, log_lines: list[str]) -> list[str]:
    ensure_dir(out_fig)
    saved: list[str] = []

    num_cols = [c for c in df.columns if is_numeric_dtype(df[c].dtype)]
    if not num_cols:
        log_lines.append("Sem colunas numéricas detectadas; pulando gráficos.")
        return saved

    pdf = safe_to_pandas_sample(df[num_cols], n=200_000)

    for c in num_cols[:4]:
        try:
            plt.figure()
            pdf[c].dropna().hist(bins=50)
            plt.title(f"Histograma: {c}")
            plt.xlabel(c)
            plt.ylabel("Frequência")
            fp = out_fig / f"hist_{c}.png"
            plt.tight_layout()
            plt.savefig(fp, dpi=150)
            plt.close()
            saved.append(fp.name)
        except Exception as e:
            log_lines.append(f"Falha ao gerar histograma {c}: {e}")

    try:
        plt.figure()
        data = [pdf[c].dropna().values for c in num_cols[:4]]
        plt.boxplot(data, labels=num_cols[:4], showfliers=True)
        plt.title("Boxplot (amostra) — primeiras colunas numéricas")
        plt.xticks(rotation=30, ha="right")
        fp = out_fig / "boxplot_primeiras_colunas.png"
        plt.tight_layout()
        plt.savefig(fp, dpi=150)
        plt.close()
        saved.append(fp.name)
    except Exception as e:
        log_lines.append(f"Falha ao gerar boxplot: {e}")

    return saved


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    env_path = repo_root / "configs" / "cap5_paths.env"
    env = read_env_file(env_path)

    parquet_path = Path(env["CAP5_METEO_PARQUET"])
    csv_path = Path(env["CAP5_METEO_CSV"])
    outdir = Path(env["CAP5_XIV_OUTDIR"])

    out_logs = outdir / "logs"
    out_tabs = outdir / "tabelas"
    out_figs = outdir / "figuras"
    out_meta = outdir / "metadata"

    for pth in [outdir, out_logs, out_tabs, out_figs, out_meta]:
        ensure_dir(pth)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = out_logs / f"run_{run_id}.log"
    meta_file = out_meta / f"run_{run_id}.json"

    log_lines: list[str] = []
    log_lines.append(f"[{now_utc_iso()}] Início item xiv (GPU). run_id={run_id}")
    log_lines.append(f"Repo: {repo_root}")
    log_lines.append(f"Parquet: {parquet_path}")
    log_lines.append(f"CSV: {csv_path}")
    log_lines.append(f"Outdir: {outdir}")

    env_info = {
        "run_id": run_id,
        "timestamp_utc": now_utc_iso(),
        "python": sys.version.replace("\n", " "),
        "platform": platform.platform(),
        "cudf": getattr(cudf, "__version__", "unknown"),
        "cupy": getattr(cp, "__version__", "unknown"),
        "numpy": getattr(np, "__version__", "unknown"),
        "git_head": run_cmd(["git", "rev-parse", "HEAD"]),
        "git_branch": run_cmd(["git", "branch", "--show-current"]),
        "nvidia_smi": run_cmd(["nvidia-smi"]),
        "paths": {
            "env_file": str(env_path),
            "parquet": str(parquet_path),
            "csv": str(csv_path),
            "outdir": str(outdir),
        },
    }

    env_info["input_hashes"] = {
        "parquet_sha256": sha256_file(parquet_path) if parquet_path.exists() else None,
        "csv_sha256": sha256_file(csv_path) if csv_path.exists() else None,
    }

    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet não encontrado: {parquet_path}")

    t0 = time.time()
    df = cudf.read_parquet(str(parquet_path))
    load_s = round(time.time() - t0, 3)
    log_lines.append(f"[{now_utc_iso()}] Carregado Parquet em GPU: linhas={len(df)} colunas={df.shape[1]} tempo_s={load_s}")

    prof = basic_profile(df)
    log_lines.append(f"Perfil básico: {prof}")

    # 1) schema / nulos
    sch = schema_table(df)
    sch_out = out_tabs / f"schema_nulos_{run_id}.csv"
    sch.to_csv(str(sch_out), index=False)
    log_lines.append(f"Gerado: {sch_out}")

    # 2) duplicatas
    dup = duplicates_info(df)
    dup_out = out_tabs / f"duplicatas_{run_id}.json"
    dup_out.write_text(json.dumps(dup, ensure_ascii=False, indent=2), encoding="utf-8")
    log_lines.append(f"Gerado: {dup_out} -> {dup}")

    # 3) describe numérico
    desc = numeric_describe(df)
    desc_out = None
    if len(desc) > 0:
        desc_out = out_tabs / f"describe_numerico_{run_id}.csv"
        desc.to_csv(str(desc_out), index=False)
        log_lines.append(f"Gerado: {desc_out}")
    else:
        log_lines.append("Sem colunas numéricas para describe().")

    # 4) quantis
    qdf = numeric_quantiles(df)
    q_out = None
    if len(qdf) > 0:
        q_out = out_tabs / f"quantis_numericos_{run_id}.csv"
        qdf.to_csv(str(q_out), index=False)
        log_lines.append(f"Gerado: {q_out}")
    else:
        log_lines.append("Sem colunas numéricas para quantis.")

    # 5) ranges/flags
    rflags = range_flags(df)
    rf_out = None
    if len(rflags) > 0:
        rf_out = out_tabs / f"ranges_flags_{run_id}.csv"
        rflags.to_csv(str(rf_out), index=False)
        log_lines.append(f"Gerado: {rf_out}")
    else:
        log_lines.append("Sem colunas numéricas para ranges/flags.")

    # 6) figuras
    figs = save_figures(df, out_figs, log_lines)
    log_lines.append(f"Figuras geradas: {figs}")

    env_info["profile"] = prof
    env_info["load_time_s"] = load_s
    env_info["outputs"] = {
        "schema_csv": str(sch_out),
        "duplicatas_json": str(dup_out),
        "describe_csv": str(desc_out) if desc_out else None,
        "quantis_csv": str(q_out) if q_out else None,
        "ranges_csv": str(rf_out) if rf_out else None,
        "figuras": figs,
        "log_file": str(log_file),
    }

    meta_file.write_text(json.dumps(env_info, ensure_ascii=False, indent=2), encoding="utf-8")
    log_lines.append(f"Metadata: {meta_file}")

    log_file.write_text("\n".join(log_lines) + "\n", encoding="utf-8")

    print(f"[OK] Item xiv concluído. Artefatos em: {outdir}")
    print(f"Log: {log_file}")
    print(f"Metadata: {meta_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
