import os
import requests
import polars as pl
from src.core.config import settings
from src.core.logging import setup_logging, logger

setup_logging()

class ContratosIngestionPipeline:
    def __init__(self):
        # Rota de consulta pública definitiva
        self.base_url = "https://pncp.gov.br/api/consulta/v1/contratos"
        self.output_dir = os.path.join(settings.LOCAL_DATA_DIR, "bronze")
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_contratos_por_data(self, data_alvo: str, pagina: int = 1) -> list:
        """
        Busca contratos publicados no PNCP em uma data específica.
        Formato obrigatório aceito pela API: AAAAMMDD (Ex: '20260601')
        """
        params = {
            "dataInicial": data_alvo,  # Sem hífens! Ex: 20260601
            "dataFinal": data_alvo,
            "pagina": pagina
        }
        
        try:
            logger.info(f"Buscando lote de contratos no PNCP para {data_alvo} (Página {pagina})...")
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            }
            response = requests.get(self.base_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            # O PNCP envelopa a lista de contratos dentro da chave 'data'
            return result.get("data", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao buscar contratos no PNCP: {e}")
            return []

    def run(self, data_alvo: str = "20260601", max_paginas: int = 1):
        logger.info("Iniciando extração de dados de contratos do PNCP...")
        all_contratos = []
        
        for page in range(1, max_paginas + 1):
            lote = self.fetch_contratos_por_data(data_alvo, pagina=page)
            if not lote:
                break
            all_contratos.extend(lote)
            
        if not all_contratos:
            logger.warning(f"Nenhum contrato retornado para a data {data_alvo}.")
            return

        # Salva o arquivo bruto na Bronze com inferência de schema completa
        # infer_schema_length=None evita quebra por tipos mistos (int vs str) na API
        df = pl.DataFrame(all_contratos, infer_schema_length=None)
        output_path = os.path.join(self.output_dir, "contratos_pncp_raw.parquet")
        df.write_parquet(output_path)
        
        logger.info(f"Sucesso! {df.height} contratos reais carregados na camada Bronze em: {output_path}")

if __name__ == "__main__":
    pipeline = ContratosIngestionPipeline()
    # Puxando o lote de contratos reais do dia 1º de Junho de 2026
    pipeline.run(data_alvo="20260601", max_paginas=1)