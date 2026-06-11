import io
import zipfile
import requests
import polars as pl
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

TSE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2024.zip"
BRONZE_PATH = "bronze/tse_receitas_raw.parquet"


class TseIngestionPipeline:

    def extrair_cnpjs_alvo(self) -> set[str]:
        cnpjs = set()

        df_alertas = storage.download_parquet("gold/analytics_alertas_corrupcao.parquet")
        if df_alertas is not None:
            cnpjs.update(
                df_alertas["documento_fornecedor_limpo"]
                .drop_nulls().unique().to_list()
            )

        df_contratos = storage.download_parquet("silver/contratos_pncp_clean.parquet")
        if df_contratos is not None:
            cnpjs.update(
                df_contratos["documento_fornecedor_limpo"]
                .drop_nulls().unique().to_list()
            )

        cnpjs_validos = {c for c in cnpjs if c.isdigit() and len(c) == 14}
        logger.info(f"CNPJs alvo para cruzamento TSE: {len(cnpjs_validos)}")
        return cnpjs_validos

    def _download_com_progresso(self, url: str) -> bytes | None:
        try:
            response = requests.get(url, timeout=120, stream=True)
            response.raise_for_status()

            total = int(response.headers.get("content-length", 0))
            baixado = 0
            chunks = []
            ultimo_log = 0

            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    chunks.append(chunk)
                    baixado += len(chunk)

                    if total:
                        pct = (baixado / total) * 100
                        # Loga a cada 10%
                        if int(pct) // 10 > ultimo_log // 10:
                            logger.info(
                                f"Download: {pct:.1f}% "
                                f"({baixado / 1024 / 1024:.1f}MB "
                                f"/ {total / 1024 / 1024:.1f}MB)"
                            )
                            ultimo_log = int(pct)
                    else:
                        # Sem content-length — loga a cada 10MB
                        mb = baixado / 1024 / 1024
                        if int(mb) % 10 == 0 and int(mb) != int((baixado - 1024 * 1024) / 1024 / 1024):
                            logger.info(f"Download: {mb:.1f}MB baixados...")

            logger.info(f"Download concluido — {baixado / 1024 / 1024:.1f}MB total.")
            return b"".join(chunks)

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao baixar arquivo TSE: {e}")
            return None

    def run(self):
        logger.info("Iniciando ingestao TSE — receitas eleitorais 2024...")

        cnpjs_alvo = self.extrair_cnpjs_alvo()
        if not cnpjs_alvo:
            logger.warning("Nenhum CNPJ alvo encontrado.")
            return

        logger.info(f"Baixando arquivo TSE...")
        content = self._download_com_progresso(TSE_URL)
        if content is None:
            return

        logger.info("Descompactando...")
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                arquivos = zf.namelist()
                logger.info(f"Arquivos no zip: {arquivos}")

                csv_files = [f for f in arquivos if "receita" in f.lower() and f.endswith(".csv")]
                if not csv_files:
                    csv_files = [f for f in arquivos if f.endswith(".csv")]

                if not csv_files:
                    logger.error("Nenhum CSV encontrado no arquivo ZIP.")
                    return

                logger.info(f"Processando: {csv_files[0]}")

                with zf.open(csv_files[0]) as f:
                    df_raw = pl.read_csv(
                        f,
                        encoding="latin1",
                        separator=";",
                        infer_schema_length=0,
                        ignore_errors=True,
                    )

        except Exception as e:
            logger.error(f"Erro ao processar ZIP: {e}")
            return

        logger.info(f"Total de registros TSE: {df_raw.height}")
        logger.info(f"Colunas disponiveis: {df_raw.columns}")

        cnpj_cols = [c for c in df_raw.columns if "cnpj" in c.lower() or "cpf" in c.lower()]
        logger.info(f"Colunas de documento encontradas: {cnpj_cols}")

        if not cnpj_cols:
            logger.warning("Nenhuma coluna de CNPJ — salvando amostra de 10000 registros.")
            df_filtrado = df_raw.head(10000)
        else:
            col_cnpj = cnpj_cols[0]
            logger.info(f"Filtrando por coluna: {col_cnpj}")

            df_filtrado = df_raw.filter(
                pl.col(col_cnpj)
                .str.replace_all(r"[\.\-\/]", "")
                .is_in(cnpjs_alvo)
            )
            logger.info(f"Registros filtrados: {df_filtrado.height}")

            if df_filtrado.height == 0:
                logger.warning("Nenhum match — salvando amostra de 5000 registros para analise do schema.")
                df_filtrado = df_raw.head(5000)

        storage.upload_or_fallback(df_filtrado, BRONZE_PATH)
        logger.info(f"Bronze TSE persistido: {BRONZE_PATH}")


if __name__ == "__main__":
    TseIngestionPipeline().run()