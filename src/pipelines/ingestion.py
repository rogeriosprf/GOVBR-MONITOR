import os
import time
import requests
import polars as pl
from src.core.config import settings
from src.core.logging import setup_logging, logger

# Inicializa as configurações de log corretamente
setup_logging()

class CeisIngestionPipeline:
    def __init__(self):
        self.base_url = f"{settings.CGU_API_BASE_URL}/ceis"
        
        # Remove espaços invisíveis ou quebras de linha da chave
        api_key = settings.CGU_API_KEY.strip()
        
        self.headers = {
            "Accept": "application/json",
            "chave-api-dados": api_key
        }
        self.output_dir = os.path.join(settings.LOCAL_DATA_DIR, "bronze")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Validação da chave nos logs de forma segura
        key_preview = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) > 8 else "CHAVE_INVALIDA"
        logger.info(f"Headers configurados. Chave identificada: {key_preview} (Tamanho: {len(api_key)})")

    def fetch_page(self, page: int) -> list:
        """Busca uma página específica de dados reais da API do CEIS."""
        params = {"pagina": page}
        try:
            response = requests.get(self.base_url, headers=self.headers, params=params, timeout=30)
            
            if response.status_code == 429:
                logger.warning("Rate limit atingido. Aguardando 15 segundos...")
                time.sleep(15)
                return self.fetch_page(page)
                
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao buscar página {page}: {e}")
            return []

    def run(self, max_pages: int = 3):
        """Executa o loop de ingestão real."""
        logger.info("Iniciando extração de dados REAIS do CEIS da API do Governo...")
        all_records = []
        page = 1

        while page <= max_pages:
            logger.info(f"Baixando página {page}...")
            data = self.fetch_page(page)
            
            if not data:
                logger.info("Fim dos dados ou erro na paginação.")
                break
                
            all_records.extend(data)
            page += 1
            time.sleep(0.6)  # Delay amigável para o servidor

        if not all_records:
            logger.warning("Nenhum dado real foi extraído. Verifique os logs acima.")
            return

        # Estrutura os dados com o Polars
        df = pl.DataFrame(all_records)
        logger.info(f"Sucesso! {df.height} registros reais carregados em memória.")

        output_path = os.path.join(self.output_dir, "ceis_raw.parquet")
        df.write_parquet(output_path)
        logger.info(f"Camada Bronze persistida com dados reais em: {output_path}")

if __name__ == "__main__":
    pipeline = CeisIngestionPipeline()
    pipeline.run(max_pages=3)