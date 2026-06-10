import time
import requests
import polars as pl
from src.core.config import settings
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()


class CnepIngestionPipeline:
    BRONZE_PATH = "bronze/cnep_raw.parquet"

    def __init__(self):
        self.base_url = f"{settings.CGU_API_BASE_URL}/cnep"
        api_key = settings.CGU_API_KEY.strip()
        self.headers = {
            "Accept": "application/json",
            "chave-api-dados": api_key
        }
        key_preview = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) > 8 else "CHAVE_INVALIDA"
        logger.info(f"CNEP Pipeline iniciado. Chave: {key_preview}")

    def fetch_page(self, page: int) -> list:
        params = {"pagina": page}
        try:
            response = requests.get(
                self.base_url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            if response.status_code == 429:
                logger.warning("Rate limit atingido. Aguardando 15 segundos...")
                time.sleep(15)
                return self.fetch_page(page)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao buscar pagina {page}: {e}")
            return []

    def run(self, max_pages: int = 15):
        logger.info("Iniciando extracao CNEP...")
        all_records = []
        for page in range(1, max_pages + 1):
            logger.info(f"Baixando pagina {page}/{max_pages}...")
            data = self.fetch_page(page)
            if not data:
                logger.info("Fim dos dados.")
                break
            all_records.extend(data)
            time.sleep(0.6)

        if not all_records:
            logger.warning("Nenhum dado extraido do CNEP.")
            return

        df = pl.DataFrame(all_records)
        logger.info(f"{df.height} registros extraidos.")
        storage.upload_or_fallback(df, self.BRONZE_PATH)
        logger.info(f"Bronze CNEP persistido: {self.BRONZE_PATH}")


if __name__ == "__main__":
    CnepIngestionPipeline().run()