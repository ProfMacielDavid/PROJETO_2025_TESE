# ============================================================
# Capítulo 5 — P1.1b
# Confirmação do dataset meteorológico (artefato versionado)
#
# Objetivo:
# - Confirmar existência, tamanho e hash (SHA256) de CSV e Parquet
# - Validar estrutura: N (registros), P (variáveis), nomes de colunas
# - Inspecionar dtypes (via pandas) e amostras
#
# Entrada:
#   resultados/capitulo_5/evidencias/meteo_bruta_PVH_2017_full.csv
#   resultados/capitulo_5/evidencias/meteo_bruta_PVH_2017_full.parquet
#
# Saídas (evidências):
#   resultados/capitulo_5/evidencias/confirmacao_p1_1b/confirmacao_resumo.txt
#   resultados/capitulo_5/evidencias/confirmacao_p1_1b/confirmacao_schema.csv
# ============================================================

from __future__ import annotations

from pathlib import Path
import hashlib
import sys
from datetime import datetime

import pandas as pd


ROOT = Path(r"C:\PROJETO_2025_TESE")
CSV_PATH = ROOT / "resultados" / "capitulo_5" / "evidencias" / "meteo_bruta_PVH_2017_full.csv"
PARQUET_PATH = ROOT / "resultados" / "capitulo_5" / "evidencias" / "meteo_bruta_PVH_2017_full.parquet"

OUT_DIR = ROOT / "resultados" / "capitulo_5" / "evidencias" / "confirmacao_p1_1b"
OUT_TXT = OUT_DIR / "confirmacao_resumo.txt"
OUT_SCHEMA = OUT_DIR / "confirmacao_schema.csv"


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def file_info(path: Path) -> dict:
    stat = path.stat()
    return {
        "path": str(path),
        "exists": path.exists(),
        "size_bytes": stat.st_size,
        "mtime_iso": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        "sha256": sha256_file(path),
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Existência
    if not CSV_PATH.exists():
        print(f"ERRO: CSV não encontrado: {CSV_PATH}")
        return 2
    if not PARQUET_PATH.exists():
        print(f"ERRO: Parquet não encontrado: {PARQUET_PATH}")
        return 2

    # 2) Informações + hash
    csv_info = file_info(CSV_PATH)
    pq_info = file_info(PARQUET_PATH)

    # 3) Leitura com pandas (confirmação CPU)
    # CSV
    df_csv = pd.read_csv(CSV_PATH)
    # Parquet
    # Nota: usa engine padrão disponível; se não houver, o erro será explícito
    df_pq = pd.read_parquet(PARQUET_PATH)

    # 4) Comparações simples
    same_shape = df_csv.shape == df_pq.shape
    same_cols = list(df_csv.columns) == list(df_pq.columns)

    # 5) Schema (colunas + dtypes)
    schema_rows = []
    for c in df_pq.columns:
        schema_rows.append({
            "coluna": c,
            "dtype_parquet": str(df_pq[c].dtype),
            "dtype_csv": str(df_csv[c].dtype) if c in df_csv.columns else "",
        })
    schema_df = pd.DataFrame(schema_rows)
    schema_df.to_csv(OUT_SCHEMA, index=False, encoding="utf-8")

    # 6) Resumo textual rastreável
    ts = datetime.now().isoformat(timespec="seconds")
    lines = []
    lines.append("CONFIRMAÇÃO DO DATASET METEOROLÓGICO — P1.1b")
    lines.append(f"Timestamp: {ts}")
    lines.append("")
    lines.append("[ARQUIVOS]")
    lines.append(f"CSV:     {csv_info['path']}")
    lines.append(f"  size_bytes: {csv_info['size_bytes']}")
    lines.append(f"  mtime:      {csv_info['mtime_iso']}")
    lines.append(f"  sha256:     {csv_info['sha256']}")
    lines.append(f"PARQUET: {pq_info['path']}")
    lines.append(f"  size_bytes: {pq_info['size_bytes']}")
    lines.append(f"  mtime:      {pq_info['mtime_iso']}")
    lines.append(f"  sha256:     {pq_info['sha256']}")
    lines.append("")
    lines.append("[ESTRUTURA]")
    lines.append(f"Shape CSV:     N={df_csv.shape[0]}  P={df_csv.shape[1]}")
    lines.append(f"Shape Parquet: N={df_pq.shape[0]}  P={df_pq.shape[1]}")
    lines.append(f"Mesma shape (CSV vs Parquet): {same_shape}")
    lines.append(f"Mesmas colunas (ordem idêntica): {same_cols}")
    lines.append("")
    lines.append("[COLUNAS]")
    lines.append("Parquet columns:")
    lines.append("  " + ", ".join(map(str, df_pq.columns)))
    lines.append("")
    lines.append("[AMOSTRA — Parquet (head 5)]")
    # imprime tabela compacta
    lines.append(df_pq.head(5).to_string(index=False))
    lines.append("")
    lines.append("[AMOSTRA — Parquet (tail 5)]")
    lines.append(df_pq.tail(5).to_string(index=False))
    lines.append("")
    lines.append(f"[SCHEMA] Arquivo gerado: {OUT_SCHEMA}")

    OUT_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("CONFIRMAÇÃO P1.1b — CONCLUÍDA")
    print(f"Saídas:")
    print(f" - {OUT_TXT}")
    print(f" - {OUT_SCHEMA}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
