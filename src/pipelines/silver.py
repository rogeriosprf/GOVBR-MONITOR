import polars as pl
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

BRONZE_PATH = "bronze/ceis_raw.parquet"
SILVER_PATH = "silver/ceis_clean.parquet"


class CeisSilverPipeline:

    def run(self):
        logger.info("Iniciando Silver CEIS...")

        df_bronze = storage.download_parquet(BRONZE_PATH)
        if df_bronze is None:
            logger.error("Bronze CEIS não encontrado no R2. Execute a ingestão primeiro.")
            return

        lf_clean = df_bronze.lazy().with_columns([
            pl.col("pessoa").struct.field("cpfFormatado").alias("cpf_raw"),
            pl.col("pessoa").struct.field("cnpjFormatado").alias("cnpj_raw"),
            pl.col("pessoa").struct.field("tipo").alias("tipo_pessoa"),
            pl.col("dataInicioSancao").str.to_date("%d/%m/%Y", strict=False),
            pl.col("dataFimSancao").str.to_date("%d/%m/%Y", strict=False),
            pl.col("sancionado").struct.field("nome").str.to_uppercase().str.strip_chars().alias("nome_sancionado"),
            pl.col("tipoSancao").struct.field("descricaoResumida").str.to_uppercase().str.strip_chars().alias("tipo_sancao"),
            pl.col("orgaoSancionador").struct.field("nome").str.to_uppercase().str.strip_chars().alias("orgao_sancionador")
        ]).with_columns([
            pl.coalesce([
                pl.when(pl.col("cnpj_raw") != "").then(pl.col("cnpj_raw")).otherwise(None),
                pl.when(pl.col("cpf_raw") != "").then(pl.col("cpf_raw")).otherwise(None)
            ]).str.replace_all(r"[\.\-\/\*]", "").alias("documento_limpo")
        ]).select([
            pl.col("id").alias("id_sancao"),
            pl.col("documento_limpo"),
            pl.col("tipo_pessoa"),
            pl.col("nome_sancionado").alias("nome_empresa_ou_pessoa"),
            pl.col("tipo_sancao"),
            pl.col("dataInicioSancao").alias("data_inicio"),
            pl.col("dataFimSancao").alias("data_fim"),
            pl.col("orgao_sancionador"),
            pl.col("numeroProcesso").alias("numero_processo")
        ])

        df_silver = lf_clean.collect()
        storage.upload_or_fallback(df_silver, SILVER_PATH)
        logger.info(f"Silver CEIS concluído — {df_silver.height} registros.")


if __name__ == "__main__":
    CeisSilverPipeline().run()