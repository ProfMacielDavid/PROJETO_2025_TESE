# ============================================================
# Classificação preliminar v4 (A2.3) — reduzir INDETERMINADO
#
# Regras adicionadas em relação ao v3:
# 1) Tudo em resultados/capitulo_5 -> TESE_PROVAVEL (evidências)
# 2) Tudo em TESE_Prof.../11_Livros -> ACERVO_LIVROS (inclui .unknown, .cpp, imagens, datasets etc.)
# 3) .xlsx/.xls fora de tese/resultados -> DADOS_EXTERNOS (controle/dados)
# 4) Arquivos sem extensão em pastas de resultados/dados -> DADOS_EXTERNOS
# 5) .qmd fora de tese/ -> DADOS_EXTERNOS (scripts/relatórios auxiliares)
#
# NÃO move arquivos. Apenas classifica.
#
# Entrada:
#   resultados/diagnostico/files_inventory.csv
#
# Saídas:
#   resultados/diagnostico/classificacao_preliminar_v4.csv
#   resultados/diagnostico/classificacao_resumo_v4.txt
# ============================================================

from pathlib import Path
import csv
from datetime import datetime

ROOT = Path(r"C:\PROJETO_2025_TESE")
INVENTORY = ROOT / "resultados" / "diagnostico" / "files_inventory.csv"
OUT_CSV = ROOT / "resultados" / "diagnostico" / "classificacao_preliminar_v4.csv"
OUT_SUMMARY = ROOT / "resultados" / "diagnostico" / "classificacao_resumo_v4.txt"

DATA_HEAVY_EXT = {
    ".tif", ".tiff", ".zip", ".nc", ".hdf5", ".he5", ".h5",
    ".shp", ".shx", ".dbf", ".prj", ".cpg", ".geojson", ".qgz", ".qml",
    ".pt", ".pth", ".state", ".gz", ".xml", ".fix", ".exe", ".com"
}

CODE_EXT = {".py", ".ipynb", ".js", ".cpp"}
DOC_EXT = {".pdf", ".docx", ".md", ".bib", ".txt", ".html"}
TABULAR_EXT = {".csv", ".parquet", ".xlsx", ".xls"}
MEDIA_EXT = {".jpg", ".jpeg", ".png", ".wav", ".mp4"}
OTHER_EXT = {".unknown", ".t"}  # extensões “estranhas” detectadas


def norm(s: str) -> str:
    return (s or "").replace("\\", "/").lower()


def classify(path_rel: str, ext: str):
    p = norm(path_rel)
    e = (ext or "").lower()

    # 0) IGNORAR Git
    if p.startswith(".git/") or "/.git/" in p:
        return None

    # 1) README
    if p == "readme.md":
        return "TESE_PROVAVEL", "README do projeto"

    # 2) Evidências e resultados do Capítulo 5 (direto no repositório)
    if p.startswith("resultados/capitulo_5/"):
        return "TESE_PROVAVEL", "Evidência/resultado do Capítulo 5 (resultados/capitulo_5)"

    # 3) Núcleo do Capítulo 5 (em árvore antiga TESE_Prof.../03_Modelagem/Resultados_Cap5)
    if "/03_modelagem/resultados_cap5/" in p:
        return "TESE_PROVAVEL", "Script/artefato do Capítulo 5 (núcleo metodológico)"

    # 4) Scripts organizados do Capítulo 5
    if p.startswith("scripts/capitulo_5/"):
        return "TESE_PROVAVEL", "Script do Capítulo 5 (organizado)"

    # 5) Tudo em 11_Livros é acervo (independe de extensão)
    if "/11_livros/" in p or "z-lib" in p or "/dlwpt-code-master/" in p:
        return "ACERVO_LIVROS", "Acervo de livros/código/datasets de terceiros (11_Livros)"

    # 6) PRISMA / MapBiomas / ATTOS (pré-processamento/dados externos)
    if any(tag in p for tag in ["/prisma", "/mapbioma", "/mapbiomas", "/attos"]):
        return "DADOS_EXTERNOS", "PRISMA/MapBiomas/ATTOS (pré-processamento/dados externos)"

    # 7) Inventários / utilitários genéricos
    if any(tag in p for tag in ["inventario", "scan_", "excluir_", "criar_subpastas"]):
        return "ACERVO_LIVROS", "Utilitário genérico / inventário"

    # 8) Arquivos tabulares (csv/parquet/xlsx/xls) fora de resultados/ -> dados externos
    if e in TABULAR_EXT:
        if p.startswith("resultados/"):
            return "TESE_PROVAVEL", "Tabela/resultado em resultados/"
        return "DADOS_EXTERNOS", "Tabela/dado tabular fora de resultados/ (provável insumo externo)"

    # 9) Extensões pesadas/auxiliares -> dados externos
    if e in DATA_HEAVY_EXT:
        return "DADOS_EXTERNOS", f"Arquivo pesado/auxiliar ({e})"

    # 10) Arquivo sem extensão: se estiver em pastas de dados/resultados -> dados externos
    if e == "" or e is None:
        if any(tag in p for tag in ["/05_dados_", "/06_resultados/", "/dados_", "/10_shape_", "/03_modelagem/"]):
            return "DADOS_EXTERNOS", "Arquivo sem extensão em pasta de dados/resultados"
        return "INDETERMINADO", "Arquivo sem extensão (revisão manual mínima)"

    # 11) QMD fora de tese -> dado externo/relatório auxiliar
    if e == ".qmd":
        if p.startswith("tese/"):
            return "TESE_PROVAVEL", "Documento de escrita em tese/"
        return "DADOS_EXTERNOS", "QMD fora de tese/ (relatório/script auxiliar)"

    # 12) Código em geral fora do núcleo -> dados externos (auxiliar)
    if e in CODE_EXT:
        if p.startswith("scripts/"):
            return "INDETERMINADO", "Código em scripts/ fora do núcleo (revisão mínima)"
        return "DADOS_EXTERNOS", "Código auxiliar fora do núcleo metodológico"

    # 13) Documentos em geral fora de tese -> acervo
    if e in DOC_EXT:
        if p.startswith("tese/") or p.startswith("resultados/"):
            return "TESE_PROVAVEL", "Documento em tese/ ou resultados/"
        return "ACERVO_LIVROS", "Documento de apoio / acervo"

    # 14) Mídia
    if e in MEDIA_EXT:
        return "ACERVO_LIVROS", "Mídia/imagem/áudio de apoio"

    # 15) Extensões incomuns -> acervo por padrão
    if e in OTHER_EXT:
        return "ACERVO_LIVROS", "Arquivo com extensão incomum (provável acervo/dataset de apoio)"

    return "INDETERMINADO", "Caso residual (revisão manual mínima)"


def main():
    if not INVENTORY.exists():
        raise FileNotFoundError(f"Inventário não encontrado: {INVENTORY}")

    rows_out = []
    counts = {"TESE_PROVAVEL": 0, "DADOS_EXTERNOS": 0, "ACERVO_LIVROS": 0, "INDETERMINADO": 0}
    ignored = 0

    with INVENTORY.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            path_rel = r.get("path", "")
            ext = r.get("extension", "")
            size_kb = r.get("size_kb", "")

            res = classify(path_rel, ext)
            if res is None:
                ignored += 1
                continue

            cat, why = res
            counts[cat] += 1

            rows_out.append({
                "path": path_rel,
                "name": r.get("name", ""),
                "extension": ext,
                "size_kb": size_kb,
                "categoria": cat,
                "justificativa": why,
            })

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["path", "name", "extension", "size_kb", "categoria", "justificativa"]
        )
        writer.writeheader()
        writer.writerows(rows_out)

    ts = datetime.now().isoformat(timespec="seconds")
    total = sum(counts.values())

    lines = [
        "CLASSIFICACAO PRELIMINAR v4 — PROJETO TESE (A2.3)",
        f"Timestamp: {ts}",
        f"Inventário: {INVENTORY}",
        f"Saída CSV:  {OUT_CSV}",
        "",
        f"Total considerados: {total}",
        f"Ignorados (.git/*): {ignored}",
        "",
        "Contagem por categoria:",
        f"  TESE_PROVAVEL: {counts['TESE_PROVAVEL']}",
        f"  DADOS_EXTERNOS: {counts['DADOS_EXTERNOS']}",
        f"  ACERVO_LIVROS: {counts['ACERVO_LIVROS']}",
        f"  INDETERMINADO: {counts['INDETERMINADO']}",
    ]

    OUT_SUMMARY.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("CLASSIFICACAO v4 CONCLUIDA")
    print("Saídas:")
    print(f" - {OUT_CSV}")
    print(f" - {OUT_SUMMARY}")


if __name__ == "__main__":
    main()
