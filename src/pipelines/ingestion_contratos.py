import time
import requests
import polars as pl
from datetime import datetime, timedelta
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()


class ContratosIngestionPipeline:
    BRONZE_PATH = "bronze/contratos_pncp_raw.parquet"

    def __init__(self):
        self.base_url = "https://pncp.gov.br/api/consulta/v1/contratos"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

    def fetch_contratos_por_data(self, data_alvo: str, pagina: int = 1, tentativas: int = 2) -> list:
        params = {
            "dataInicial": data_alvo,
            "dataFinal": data_alvo,
            "pagina": pagina
        }
        for tentativa in range(1, tentativas + 1):
            try:
                logger.info(f"Buscando contratos PNCP — {data_alvo} pagina {pagina} (tentativa {tentativa})...")
                response = requests.get(
                    self.base_url,
                    params=params,
                    headers=self.headers,
                    timeout=60
                )
                response.raise_for_status()
                return response.json().get("data", [])
            except requests.exceptions.RequestException as e:
                logger.warning(f"Tentativa {tentativa} falhou: {e}")
                if tentativa < tentativas:
                    time.sleep(3 * tentativa)

        logger.error(f"Todas as tentativas falharam para {data_alvo} pagina {pagina}.")
        return []

    def run(self, dias: int = 7, max_paginas_por_dia: int = 5):
        logger.info(f"Iniciando extracao PNCP — ultimos {dias} dias...")
        all_contratos = []
        data_base = datetime.today()

        for i in range(dias):
            data_alvo = (data_base - timedelta(days=i)).strftime("%Y%m%d")
            dia_com_dados = False

            for pagina in range(1, max_paginas_por_dia + 1):
                lote = self.fetch_contratos_por_data(data_alvo, pagina)
                if not lote:
                    break
                all_contratos.extend(lote)
                dia_com_dados = True
                time.sleep(0.3)

            if not dia_com_dados:
                logger.warning(f"Sem dados para {data_alvo}, pulando.")

        if not all_contratos:
            logger.warning("Nenhum contrato extraido do PNCP.")
            return

        df = pl.DataFrame(all_contratos, infer_schema_length=None)
        logger.info(f"{df.height} contratos extraidos.")
        storage.upload_or_fallback(df, self.BRONZE_PATH)
        logger.info(f"Bronze Contratos persistido: {self.BRONZE_PATH}")


if __name__ == "__main__":
    ContratosIngestionPipeline().run()