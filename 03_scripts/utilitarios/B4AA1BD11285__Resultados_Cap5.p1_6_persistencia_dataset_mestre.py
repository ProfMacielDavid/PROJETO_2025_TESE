# ============================================================
# Etapa P1.6 — Persistência do Dataset Meteorológico Mestre
# Capítulo 5 — Modelagem Híbrida IA + Físico
# ============================================================

import cudf
from pathlib import Path

BASE_DIR = Path("/mnt/c/PROJETO_2025_TESE")
DATA_DIR = BASE_DIR / "data" / "meteorologia"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Entrada: saída da P1.5 (ordem fixa)
CANDIDATOS_INPUT = [
    DATA_DIR / "dataset_meteorologico_p1_5_ordenado.parquet",
    DATA_DIR / "dataset_meteorologico_p1_5.parquet",
    DATA_DIR / "dataset_meteorologico_ordenado.parquet",
    DATA_DIR / "dataset_p1_5_ordenado.parquet",
]

OUTPUT_PARQUET = DATA_DIR / "dataset_mestre_meteorologico.parquet"

# 1) Seleciona o primeiro arquivo existente (determinístico)
INPUT_PARQUET = None
for p in CANDIDATOS_INPUT:
    if p.exists():
        INPUT_PARQUET = p
        break

if INPUT_PARQUET is None:
    raise FileNotFoundError(
        "Arquivo de entrada da P1.5 não encontrado nos caminhos esperados.\n\n"
        "Caminhos verificados (ordem):\n"
        + "\n".join([f"- {p}" for p in CANDIDATOS_INPUT])
        + "\n"
    )

# 2) Carrega, valida e ordena
df = cudf.read_parquet(str(INPUT_PARQUET))

print("Dataset de entrada (P1.5) localizado:")
print(f"{INPUT_PARQUET}")
print(f"Linhas: {len(df)} | Colunas: {len(df.columns)}")

if "date_time" not in df.columns:
    raise KeyError("Campo 'date_time' ausente no dataset de entrada (P1.5).")

df = df.sort_values("date_time").reset_index(drop=True)

if not df["date_time"].is_monotonic_increasing:
    raise ValueError("Série temporal não está monotonicamente crescente após ordenação.")

print("Validação temporal final confirmada (date_time crescente).")

# 3) Persiste o dataset mestre
df.to_parquet(str(OUTPUT_PARQUET), index=False)

print("==============================================")
print("Etapa P1.6 concluída com sucesso.")
print(f"Dataset mestre persistido em:\n{OUTPUT_PARQUET}")
print("==============================================")
