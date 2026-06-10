import time
import requests
import polars as pl
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()


class CnpjIngestionPipeline:
    BRONZE_PATH = "bronze/cnpj_empresas_raw.parquet"
    BASE_URL = "https://brasilapi.com.br/api/cnpj/v1"

    def fetch_cnpj(self, cnpj: str) -> dict | None:
        """Busca dados de um CNPJ na BrasilAPI."""
        try:
            response = requests.get(
                f"{self.BASE_URL}/{cnpj}",
                timeout=15
            )
            if response.status_code == 404:
                logger.warning(f"CNPJ nao encontrado: {cnpj}")
                return None
            if response.status_code == 429:
                logger.warning("Rate limit atingido. Aguardando 30 segundos...")
                time.sleep(30)
                return self.fetch_cnpj(cnpj)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao buscar CNPJ {cnpj}: {e}")
            return None

    def extrair_cnpjs_alvo(self) -> list[str]:
        """Extrai CNPJs apenas dos alertas gerados — foco nos suspeitos."""
        cnpjs = set()

        df_alertas = storage.download_parquet("gold/analytics_alertas_corrupcao.parquet")
        if df_alertas is not None:
                    cnpjs.update(
                    df_alertas["documento_fornecedor_limpo"]
                .drop_nulls()
                .unique()
                .to_list()
                            )
        logger.info(f"CNPJs alvo dos alertas: {len(cnpjs)}")

        cnpjs_validos = [c for c in cnpjs if c.isdigit() and len(c) == 14]
        logger.info(f"Total CNPJs para consulta: {len(cnpjs_validos)}")
        return cnpjs_validos

    def run(self):
        logger.info("Iniciando ingestao CNPJ via BrasilAPI...")

        cnpjs_alvo = self.extrair_cnpjs_alvo()
        if not cnpjs_alvo:
            logger.warning("Nenhum CNPJ alvo encontrado.")
            return

        resultados = []
        total = len(cnpjs_alvo)

        for i, cnpj in enumerate(cnpjs_alvo, 1):
            logger.info(f"Consultando CNPJ {i}/{total}: {cnpj}")
            dados = self.fetch_cnpj(cnpj)
            if dados:
                resultados.append(dados)
            time.sleep(0.5)  # respeita rate limit da BrasilAPI

        if not resultados:
            logger.warning("Nenhum dado retornado pela BrasilAPI.")
            return

        df = pl.DataFrame(resultados, infer_schema_length=None)
        logger.info(f"{df.height} empresas consultadas.")
        storage.upload_or_fallback(df, self.BRONZE_PATH)
        logger.info(f"Bronze CNPJ persistido: {self.BRONZE_PATH}")


if __name__ == "__main__":
    CnpjIngestionPipeline().run()