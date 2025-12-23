# Evidência — Item xiv: Validação estrutural e estatística da tabela meteorológica em GPU

Este diretório consolida os artefatos gerados pelo script de validação em GPU (Capítulo 5, item xiv).

## Entradas (caminhos canônicos)
- Parquet: `dados/capitulo_5/meteorologia/meteo_bruta_PVH_2017_full.parquet`
- CSV: `dados/capitulo_5/meteorologia/meteo_bruta_PVH_2017_full.csv`
- Hashes: `dados/capitulo_5/meteorologia/SHA256SUMS.txt`

## Saídas esperadas
- `logs/`: logs de execução e ambiente
- `tabelas/`: tabelas CSV/TSV com resumo estatístico, nulos, duplicatas, tipos, ranges
- `figuras/`: gráficos exportados (PNG/SVG) para tese
- `metadata/`: metadados do run (JSON), configurações e versões

## Configuração
Os caminhos são definidos em: `configs/cap5_paths.env`
