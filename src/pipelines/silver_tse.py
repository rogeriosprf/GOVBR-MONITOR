import polars as pl
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

BRONZE_PATH = "bronze/tse_receitas_raw.parquet"
SILVER_PATH = "silver/tse_receitas_clean.parquet"


class TseSilverPipeline:

    def run(self):
        logger.info("Iniciando Silver TSE...")

        df = storage.download_parquet(BRONZE_PATH)
        if df is None:
            logger.error("Bronze TSE nao encontrado no R2.")
            return

        df_silver = df.select([
            pl.col("NR_CPF_CNPJ_DOADOR")
                .str.replace_all(r"[\.\-\/]", "")
                .alias("documento_doador"),
            pl.col("NM_DOADOR_RFB")
                .str.to_uppercase().str.strip_chars()
                .alias("nome_doador"),
            pl.col("NM_CANDIDATO")
                .str.to_uppercase().str.strip_chars()
                .alias("nome_candidato"),
            pl.col("NR_CPF_CANDIDATO").alias("cpf_candidato"),
            pl.col("SG_PARTIDO").alias("partido"),
            pl.col("NM_PARTIDO").alias("nome_partido"),
            pl.col("DS_CARGO").alias("cargo_candidato"),
            pl.col("SG_UF").alias("uf_candidato"),
            pl.col("NM_UE").alias("municipio_candidato"),
            pl.col("DT_RECEITA")
                .str.to_date("%d/%m/%Y", strict=False)
                .alias("data_doacao"),
            pl.col("VR_RECEITA")
                .str.replace_all(r"\.", "")
                .str.replace(",", ".")
                .cast(pl.Float64, strict=False)
                .alias("valor_doacao"),
            pl.col("DS_ORIGEM_RECEITA").alias("origem_receita"),
            pl.col("DS_NATUREZA_RECEITA").alias("natureza_receita"),
            pl.col("DS_CNAE_DOADOR").alias("cnae_doador"),
            pl.col("SG_UF_DOADOR").alias("uf_doador"),
            pl.col("NM_MUNICIPIO_DOADOR").alias("municipio_doador"),
            pl.col("NR_RECIBO_DOACAO").alias("numero_recibo"),
            pl.col("AA_ELEICAO").alias("ano_eleicao"),
        ])

        storage.upload_or_fallback(df_silver, SILVER_PATH)
        logger.info(f"Silver TSE concluido — {df_silver.height} registros.")
        logger.info(f"Valor total doado: R$ {df_silver['valor_doacao'].sum():,.2f}")


if __name__ == "__main__":
    TseSilverPipeline().run()