import polars as pl
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

BRONZE_PATH = "bronze/cnpj_empresas_raw.parquet"
SILVER_PATH = "silver/cnpj_empresas_clean.parquet"
SILVER_SOCIOS_PATH = "silver/cnpj_socios_clean.parquet"


class CnpjSilverPipeline:

    def run(self):
        logger.info("Iniciando Silver CNPJ...")

        df = storage.download_parquet(BRONZE_PATH)
        if df is None:
            logger.error("Bronze CNPJ nao encontrado no R2.")
            return

        # --- Empresa ---
        df_empresas = df.select([
            pl.col("cnpj").str.replace_all(r"[\.\-\/]", "").alias("documento_limpo"),
            pl.col("razao_social").str.to_uppercase().str.strip_chars(),
            pl.col("nome_fantasia").str.to_uppercase().str.strip_chars(),
            pl.col("descricao_situacao_cadastral").str.to_uppercase().alias("situacao_cadastral"),
            pl.col("descricao_motivo_situacao_cadastral").str.to_uppercase().alias("motivo_situacao"),
            pl.col("data_inicio_atividade").str.to_date("%Y-%m-%d", strict=False),
            pl.col("data_situacao_cadastral").str.to_date("%Y-%m-%d", strict=False),
            pl.col("porte").str.to_uppercase(),
            pl.col("natureza_juridica").str.to_uppercase(),
            pl.col("capital_social"),
            pl.col("cnae_fiscal_descricao").str.to_uppercase().alias("atividade_principal"),
            pl.col("opcao_pelo_simples"),
            pl.col("opcao_pelo_mei"),
            pl.col("descricao_identificador_matriz_filial").str.to_uppercase().alias("matriz_ou_filial"),
            pl.col("logradouro").str.to_uppercase().str.strip_chars(),
            pl.col("numero"),
            pl.col("bairro").str.to_uppercase().str.strip_chars(),
            pl.col("municipio").str.to_uppercase().str.strip_chars(),
            pl.col("uf"),
            pl.col("cep"),
        ])

        storage.upload_or_fallback(df_empresas, SILVER_PATH)
        logger.info(f"Silver Empresas concluido — {df_empresas.height} registros.")

        # --- Sócios --- explode a lista qsa
        df_socios = (
            df.select([
                pl.col("cnpj").str.replace_all(r"[\.\-\/]", "").alias("documento_empresa"),
                pl.col("razao_social").str.to_uppercase().str.strip_chars(),
                pl.col("qsa"),
            ])
            .explode("qsa")
            .with_columns([
                pl.col("qsa").struct.field("nome_socio").str.to_uppercase().str.strip_chars(),
                pl.col("qsa").struct.field("cnpj_cpf_do_socio").str.replace_all(r"[\.\-\/\*]", "").alias("documento_socio"),
                pl.col("qsa").struct.field("qualificacao_socio").str.to_uppercase(),
                pl.col("qsa").struct.field("faixa_etaria"),
                pl.col("qsa").struct.field("data_entrada_sociedade").str.to_date("%Y-%m-%d", strict=False),
            ])
            .select([
                pl.col("documento_empresa"),
                pl.col("razao_social"),
                pl.col("nome_socio"),
                pl.col("documento_socio"),
                pl.col("qualificacao_socio"),
                pl.col("faixa_etaria"),
                pl.col("data_entrada_sociedade"),
            ])
        )

        storage.upload_or_fallback(df_socios, SILVER_SOCIOS_PATH)
        logger.info(f"Silver Socios concluido — {df_socios.height} socios extraidos.")

        # Preview dos alertas enriquecidos
        logger.info("--- EMPRESAS SUSPEITAS ---")
        for row in df_empresas.iter_rows(named=True):
            logger.info(
                f"{row['razao_social']} | "
                f"Situacao: {row['situacao_cadastral']} | "
                f"Abertura: {row['data_inicio_atividade']} | "
                f"Capital: R$ {row['capital_social']:,.2f} | "
                f"{row['municipio']}/{row['uf']}"
            )

        logger.info("--- SOCIOS ---")
        for row in df_socios.iter_rows(named=True):
            logger.info(
                f"{row['razao_social']} → "
                f"{row['nome_socio']} | "
                f"Doc: {row['documento_socio']} | "
                f"Entrada: {row['data_entrada_sociedade']}"
            )


if __name__ == "__main__":
    CnpjSilverPipeline().run()