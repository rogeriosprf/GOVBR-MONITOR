# GOVBR-Monitor 🛡️

Pipeline de dados de alta performance e monitor de auditoria de dados públicos utilizando a arquitetura Medallion.

## 🏗️ Arquitetura e Stack
- **Ingestão:** Python (Requests) consumindo APIs da CGU/TSE.
- **Armazenamento:** Azure Data Lake Storage (Arquivos Parquet comprimidos).
- **Processamento:** **Polars** (Transformação escalável e Lazy Evaluation).
- **Persistência:** Azure SQL Database (Modelagem Dimensional para a camada Gold).
- **Exposição:** FastAPI (Backend assíncrono).
- **Visualização:** Streamlit Dashboard.

## 🚀 Como Rodar Localmente
1. Copie o arquivo `.env.example` para `.env` e ajuste as variáveis.
2. Instale as dependências com o Poetry:
   ```bash
   poetry install