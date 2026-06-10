# GOVBR-MONITOR 🔍

> Pipeline de dados em nuvem para detecção de padrões de corrupção em contratos públicos brasileiros.

[![Python](https://img.shields.io/badge/Python-3.14-blue)](https://python.org)
[![Polars](https://img.shields.io/badge/Polars-LazyFrame-orange)](https://pola.rs)
[![Cloudflare R2](https://img.shields.io/badge/Storage-Cloudflare%20R2-F38020)](https://developers.cloudflare.com/r2)
[![License: MIT](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## O que é

O GOVBR-MONITOR é um pipeline de engenharia de dados que ingere, transforma e cruza bases de dados públicas do governo brasileiro para identificar irregularidades em contratos públicos — como empresas impedidas contratando com órgãos federais.

**Resultado real:** em uma amostragem de 1.000 contratos públicos dos últimos 7 dias cruzados com 225 sanções ativas do CEIS, o sistema detectou 1 contrato crítico: a empresa **MAX-FER TOOLS COMERCIAL LTDA** (CNPJ 54.793.517/0001-04), impedida de contratar com o governo, assinou contrato com o **Instituto Federal de Educação, Ciência e Tecnologia do Norte de Minas Gerais** em abril de 2026.

---

## Arquitetura

```
Fontes Públicas
├── CGU / Portal da Transparência — CEIS (empresas sancionadas)
└── PNCP — Contratos públicos federais

         ↓ ingestão com retry e backoff

Cloudflare R2 (Data Lake)
├── bronze/   → dados brutos (Parquet comprimido zstd)
├── silver/   → dados limpos e tipados
└── gold/     → cruzamentos analíticos e alertas

         ↓ transformação com Polars LazyFrame

Alertas de Irregularidade
├── CRITICO  → empresa impedida contratou durante vigência da sanção
└── HISTORICO → empresa com sanções correlatas (fora do período ativo)
```

### Stack

| Camada | Tecnologia |
|---|---|
| Linguagem | Python 3.14 |
| Transformação | Polars (LazyFrame + zstd) |
| Storage | Cloudflare R2 (S3-compatible) |
| Configuração | Pydantic Settings |
| Containerização | Docker |
| Ingestão | requests + retry com backoff progressivo |

---

## Fontes de dados

| Base | Órgão | Endpoint |
|---|---|---|
| CEIS | CGU / Portal da Transparência | `api.portaldatransparencia.gov.br/api-de-dados/ceis` |
| Contratos | PNCP | `pncp.gov.br/api/consulta/v1/contratos` |

> A API do PNCP apresenta instabilidades ocasionais. O pipeline implementa retry com backoff progressivo e fallback local para garantir resiliência.

---

## Medallion Architecture

### Bronze
Dados brutos exatamente como retornados pelas APIs. Preserva structs aninhados, tipos originais e todos os campos. Comprimido em Parquet/zstd no R2.

### Silver
Dados limpos e padronizados:
- Extração de CPF/CNPJ de structs aninhados
- Normalização de strings (uppercase, strip)
- Parse de datas para tipo `Date` nativo
- Documento unificado (CPF ou CNPJ) em coluna limpa

### Gold
Cruzamentos analíticos:
- `fact_ceis_sancoes.parquet` — sanções com status ativo/expirado
- `dm_sancoes_por_orgao.parquet` — agregações por órgão sancionador
- `analytics_alertas_corrupcao.parquet` — contratos com fornecedores sancionados

---

## Como executar

### Pré-requisitos

- Python 3.12+
- Poetry
- Conta Cloudflare com R2 ativo
- Chave de API do Portal da Transparência ([obter aqui](https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email))

### Instalação

```bash
git clone https://github.com/rogeriosprf/GOVBR-MONITOR.git
cd GOVBR-MONITOR
poetry install
```

### Configuração

Crie um arquivo `.env` na raiz do projeto:

```env
CGU_API_KEY=sua_chave_aqui
CLOUDFLARE_ACCOUNT_ID=seu_account_id
CLOUDFLARE_ACCESS_KEY_ID=sua_access_key
CLOUDFLARE_SECRET_ACCESS_KEY=sua_secret_key
CLOUDFLARE_BUCKET_NAME=govbr-datalake
```

Veja `.env.example` para referência.

### Execução

```bash
# Pipeline completo
python -m src.main

# Pipelines individuais
python -m src.pipelines.ingestion          # Bronze CEIS
python -m src.pipelines.silver             # Silver CEIS
python -m src.pipelines.gold               # Gold CEIS
python -m src.pipelines.ingestion_contratos # Bronze Contratos
python -m src.pipelines.silver_contratos   # Silver Contratos
python -m src.pipelines.gold_monitor       # Cruzamento final
```

### Docker

```bash
docker-compose up --build
```

---

## Estrutura do projeto

```
govbr-monitor/
├── src/
│   ├── core/
│   │   ├── config.py       # Pydantic Settings + R2 endpoint
│   │   ├── storage.py      # Cliente R2 com fallback local
│   │   └── logging.py      # Configuração de logs
│   ├── pipelines/
│   │   ├── ingestion.py          # Bronze CEIS
│   │   ├── silver.py             # Silver CEIS
│   │   ├── gold.py               # Gold CEIS + KPIs
│   │   ├── ingestion_contratos.py # Bronze PNCP
│   │   ├── silver_contratos.py   # Silver Contratos
│   │   └── gold_monitor.py       # Cruzamento CEIS x PNCP
│   ├── api/
│   │   └── main.py         # FastAPI (em desenvolvimento)
│   ├── dashboard/
│   │   └── app.py          # Streamlit (em desenvolvimento)
│   └── main.py             # Orquestrador principal
├── tests/
├── .env.example
├── docker-compose.yml
├── Dockerfile
└── pyproject.toml
```

---

## Próximas camadas (roadmap)

- [ ] **CNEP** — Cadastro Nacional de Empresas Punidas (CGU)
- [ ] **CNPJ Receita Federal** — sócios e situação cadastral
- [ ] **TSE** — financiamento eleitoral cruzado com contratos
- [ ] **Servidores públicos** — detecção de conflito de interesse
- [ ] **Dashboard Streamlit** — visualização de alertas
- [ ] **FastAPI** — endpoints de consulta pública
- [ ] **GitHub Actions** — pipeline automatizado diariamente

---

## Autor

**Rogerio** — Engenheiro de Dados  
[GitHub](https://github.com/rogeriosprf) · [Portfolio](https://rogeriosprf.github.io/portifolio/)

---

> *Dados públicos são patrimônio público. Este projeto usa apenas fontes abertas e oficiais do governo brasileiro.*