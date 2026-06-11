import polars as pl
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

BRONZE_PATH = "bronze/contratos_pncp_raw.parquet"
SILVER_PATH = "silver/contratos_pncp_clean.parquet"


class ContratosSilverPipeline:

    def run(self):
        logger.info("Iniciando Silver Contratos PNCP...")

        df_bronze = storage.download_parquet(BRONZE_PATH)
        if df_bronze is None:
            logger.error("Bronze Contratos não encontrado no R2. Execute a ingestão primeiro.")
            return

        lf_clean = df_bronze.lazy().with_columns([
            pl.col("niFornecedor").str.replace_all(r"[\.\-\/\* ]", "").alias("documento_fornecedor_limpo"),
            pl.col("nomeRazaoSocialFornecedor").str.to_uppercase().str.strip_chars().alias("nome_fornecedor"),
            pl.col("objetoContrato").str.strip_chars().alias("objeto_contrato"),
            pl.col("orgaoEntidade").struct.field("cnpj").str.replace_all(r"[\.\-\/\* ]", "").alias("cnpj_orgao_comprador"),
            pl.col("orgaoEntidade").struct.field("razaoSocial").str.to_uppercase().str.strip_chars().alias("nome_orgao_comprador"),
            pl.col("dataAssinatura").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("data_assinatura"),
            pl.col("dataVigenciaInicio").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("data_vigencia_inicio"),
            pl.col("dataVigenciaFim").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("data_vigencia_fim"),
            pl.col("tipoContrato").struct.field("nome").str.to_uppercase().str.strip_chars().alias("tipo_contrato")
        ]).select([
            pl.col("numeroControlePNCP").alias("id_contrato_pncp"),
            pl.col("documento_fornecedor_limpo"),
            pl.col("tipoPessoa").alias("tipo_pessoa_fornecedor"),
            pl.col("nome_fornecedor"),
            pl.col("cnpj_orgao_comprador"),
            pl.col("nome_orgao_comprador"),
            pl.col("tipo_contrato"),
            pl.col("data_assinatura"),
            pl.col("data_vigencia_inicio"),
            pl.col("data_vigencia_fim"),
            pl.col("valorGlobal").alias("valor_global_contrato")
        ])

        df_silver = lf_clean.collect()
        storage.upload_or_fallback(df_silver, SILVER_PATH)
        logger.info(f"Silver Contratos concluído — {df_silver.height} registros.")


if __name__ == "__main__":
    ContratosSilverPipeline().run()