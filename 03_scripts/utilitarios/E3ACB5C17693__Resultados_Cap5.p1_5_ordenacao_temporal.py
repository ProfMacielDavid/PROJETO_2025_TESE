#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Etapa P1.5 — Ordenação temporal

c. Entrada:
- Dataset meteorológico com campo date_time já criado e validado nas etapas anteriores.

d. Campo:
- date_time

e. Operações:
f. Ordenação crescente da série temporal
g. Garantia de sequência temporal coerente (monotonicidade temporal)

h. Saída:
i. Série temporal ordenada (arquivo parquet)
"""

import os
import sys
import argparse
import cudf


def parse_args():
    p = argparse.ArgumentParser(description="Resultados Cap5 — P1.5 Ordenação temporal (GPU/cuDF)")
    p.add_argument("--input", required=True, help="Arquivo de entrada (.parquet)")
    p.add_argument("--output", required=True, help="Arquivo de saída (.parquet)")
    p.add_argument("--date-col", default="date_time", help="Coluna temporal (default: date_time)")
    return p.parse_args()


def main():
    args = parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {args.input}")

    print("\n[P1.5] Etapa P1.5 — Ordenação temporal")
    print(f"[P1.5] Entrada: {args.input}")
    print(f"[P1.5] Saída  : {args.output}")
    print(f"[P1.5] Campo  : {args.date_col}\n")

    # c) Entrada
    df = cudf.read_parquet(args.input)
    print(f"[P1.5] Linhas (antes): {len(df):,}")
    print(f"[P1.5] Colunas: {list(df.columns)}\n")

    # d) Campo date_time
    if args.date_col not in df.columns:
        raise KeyError(f"Coluna '{args.date_col}' não encontrada no dataset.")

    # Converter para datetime (compatível com cuDF sem errors='coerce' em Series)
    try:
        df[args.date_col] = cudf.to_datetime(df[args.date_col])
    except Exception as conv_err:
        raise RuntimeError(
            f"Falha na conversão de '{args.date_col}' para datetime no cuDF. "
            f"Erro: {type(conv_err).__name__}: {conv_err}"
        )

    # Checar nulos após conversão
    nulls = int(df[args.date_col].isna().sum())
    if nulls > 0:
        raise ValueError(
            f"Falha: {nulls} valores nulos em '{args.date_col}' após conversão para datetime."
        )

    # Evidências pré-ordenação
    dt_min = df[args.date_col].min()
    dt_max = df[args.date_col].max()
    dup_count = int(df.duplicated(subset=[args.date_col]).sum())
    print(f"[P1.5] date_time mínimo (antes): {dt_min}")
    print(f"[P1.5] date_time máximo  (antes): {dt_max}")
    print(f"[P1.5] Duplicatas em date_time (antes): {dup_count:,}\n")

    # f) Ordenação crescente
    df_sorted = df.sort_values(by=args.date_col, ascending=True)

    # g) Garantia de sequência temporal coerente (monotonicidade não decrescente)
    s = df_sorted[args.date_col]
    is_mono = bool((s.shift(-1) >= s).fillna(True).all())
    if not is_mono:
        raise RuntimeError("[P1.5] Falha: sequência temporal incoerente (não monotônica) após ordenação.")

    # Evidências pós-ordenação
    dt_min2 = df_sorted[args.date_col].min()
    dt_max2 = df_sorted[args.date_col].max()
    dup_count2 = int(df_sorted.duplicated(subset=[args.date_col]).sum())

    print(f"[P1.5] date_time mínimo (depois): {dt_min2}")
    print(f"[P1.5] date_time máximo  (depois): {dt_max2}")
    print(f"[P1.5] Duplicatas em date_time (depois): {dup_count2:,}")
    print("[P1.5] Monotonicidade temporal: OK (não decrescente)\n")

    # h/i) Saída
    out_dir = os.path.dirname(os.path.abspath(args.output))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_sorted.to_parquet(args.output, index=False)
    print(f"[P1.5] Arquivo salvo: {args.output}")
    print("[P1.5] Conclusão: Série temporal ordenada.\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[P1.5] ERRO: {type(e).__name__}: {e}\n", file=sys.stderr)
        sys.exit(1)
