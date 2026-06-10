import polars as pl
from datetime import date
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

BRONZE_PATH = "bronze/cnep_raw.parquet"
SILVER_PATH = "silver/cnep_clean.parquet"


class CnepSilverPipeline:

    def run(self):
        logger.info("Iniciando Silver CNEP...")

        df_bronze = storage.download_parquet(BRONZE_PATH)
        if df_bronze is None:
            logger.error("Bronze CNEP não encontrado no R2. Execute a ingestão primeiro.")
            return

        today = date.today()

        df_silver = df_bronze.lazy().with_columns([
            # Extrai CPF ou CNPJ
            pl.col("pessoa").struct.field("cpfFormatado").alias("cpf_raw"),
            pl.col("pessoa").struct.field("cnpjFormatado").alias("cnpj_raw"),
            pl.col("pessoa").struct.field("tipo").alias("tipo_pessoa"),

            # Datas
            pl.col("dataInicioSancao").str.to_date("%d/%m/%Y", strict=False),
            pl.col("dataFimSancao").str.to_date("%d/%m/%Y", strict=False),
            pl.col("dataTransitadoJulgado").str.to_date("%d/%m/%Y", strict=False),

            # Strings normalizadas
            pl.col("sancionado").struct.field("nome").str.to_uppercase().str.strip_chars().alias("nome_sancionado"),
            pl.col("tipoSancao").struct.field("descricaoResumida").str.to_uppercase().str.strip_chars().alias("tipo_sancao"),
            pl.col("orgaoSancionador").struct.field("nome").str.to_uppercase().str.strip_chars().alias("orgao_sancionador"),
            pl.col("orgaoSancionador").struct.field("esfera").str.to_uppercase().str.strip_chars().alias("esfera_sancionador"),

            # Valor da multa — limpa e converte para float
            pl.col("valorMulta")
                .str.replace_all(r"[R\$\s\.]", "")
                .str.replace(",", ".")
                .cast(pl.Float64, strict=False)
                .alias("valor_multa"),

        ]).with_columns([
            # Documento unificado limpo
            pl.coalesce([
                pl.when(pl.col("cnpj_raw") != "").then(pl.col("cnpj_raw")).otherwise(None),
                pl.when(pl.col("cpf_raw") != "").then(pl.col("cpf_raw")).otherwise(None),
            ]).str.replace_all(r"[\.\-\/\*]", "").alias("documento_limpo"),

            # Status da punição
            pl.when(
                (pl.col("dataInicioSancao") <= today) &
                (
                    (pl.col("dataFimSancao").is_null()) |
                    (pl.col("dataFimSancao") >= today)
                )
            )
            .then(pl.lit("ATIVA"))
            .otherwise(pl.lit("EXPIRADA"))
            .alias("status_punicao"),

        ]).select([
            pl.col("id").alias("id_punicao"),
            pl.col("documento_limpo"),
            pl.col("tipo_pessoa"),
            pl.col("nome_sancionado").alias("nome_empresa_ou_pessoa"),
            pl.col("tipo_sancao"),
            pl.col("dataInicioSancao").alias("data_inicio"),
            pl.col("dataFimSancao").alias("data_fim"),
            pl.col("dataTransitadoJulgado").alias("data_transito_julgado"),
            pl.col("orgao_sancionador"),
            pl.col("esfera_sancionador"),
            pl.col("valor_multa"),
            pl.col("status_punicao"),
            pl.col("numeroProcesso").alias("numero_processo"),
        ]).collect()

        storage.upload_or_fallback(df_silver, SILVER_PATH)
        logger.info(f"Silver CNEP concluido — {df_silver.height} registros.")
        ativas = df_silver.filter(pl.col("status_punicao") == "ATIVA").height
        logger.info(f"Punicoes ativas: {ativas} | Expiradas: {df_silver.height - ativas}")


if __name__ == "__main__":
    CnepSilverPipeline().run()