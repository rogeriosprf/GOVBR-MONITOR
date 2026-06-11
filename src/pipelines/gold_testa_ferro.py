import polars as pl
from datetime import date
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

SILVER_SOCIOS_PATH = "silver/cnpj_socios_clean.parquet"
SILVER_CEIS_PATH = "silver/ceis_clean.parquet"
SILVER_CONTRATOS_PATH = "silver/contratos_pncp_clean.parquet"
GOLD_TESTA_FERRO_PATH = "gold/analytics_testa_ferro.parquet"


class TestaFerroGoldPipeline:

    def run(self):
        logger.info("Iniciando deteccao de testa de ferro...")

        df_socios = storage.download_parquet(SILVER_SOCIOS_PATH)
        df_ceis = storage.download_parquet(SILVER_CEIS_PATH)
        df_contratos = storage.download_parquet(SILVER_CONTRATOS_PATH)

        if any(df is None for df in [df_socios, df_ceis, df_contratos]):
            logger.error("Dados necessarios nao encontrados no R2.")
            return

        today = date.today()

        # --- Passo 1 ---
        # Identifica sócios de empresas sancionadas no CEIS
        socios_de_empresas_ceis = (
            df_ceis.lazy()
            .filter(pl.col("tipo_pessoa") == "J")
            .with_columns([
                pl.when(
                    (pl.col("data_inicio") <= today) &
                    (
                        (pl.col("data_fim").is_null()) |
                        (pl.col("data_fim") >= today)
                    )
                )
                .then(pl.lit("ATIVA"))
                .otherwise(pl.lit("EXPIRADA"))
                .alias("status_sancao")
            ])
            .select([
                pl.col("documento_limpo").alias("cnpj_empresa_ceis"),
                pl.col("nome_empresa_ou_pessoa").alias("nome_empresa_ceis"),
                pl.col("tipo_sancao"),
                pl.col("data_inicio").alias("data_inicio_sancao"),
                pl.col("data_fim").alias("data_fim_sancao"),
                pl.col("status_sancao"),
            ])
            .join(
                df_socios.lazy().select([
                    pl.col("documento_empresa"),
                    pl.col("nome_socio"),
                    pl.col("documento_socio"),
                    pl.col("data_entrada_sociedade"),
                ]),
                left_on="cnpj_empresa_ceis",
                right_on="documento_empresa",
                how="inner"
            )
            .collect()
        )

        if socios_de_empresas_ceis.height == 0:
            logger.info("Nenhum socio de empresa sancionada encontrado.")
            return

        logger.info(f"Socios de empresas no CEIS identificados: {socios_de_empresas_ceis.height}")

        # --- Passo 2 ---
        # Verifica se esses sócios aparecem em empresas contratando com o governo
        df_socios_em_contratos = (
            df_socios.lazy()
            .join(
                df_contratos.lazy().select([
                    pl.col("documento_fornecedor_limpo"),
                    pl.col("nome_fornecedor"),
                    pl.col("nome_orgao_comprador"),
                    pl.col("valor_global_contrato"),
                    pl.col("data_assinatura"),
                    pl.col("id_contrato_pncp"),
                ]),
                left_on="documento_empresa",
                right_on="documento_fornecedor_limpo",
                how="inner"
            )
            .collect()
        )

        # --- Passo 3 ---
        # Cruza — mesmo sócio em empresa do CEIS e em empresa contratando
        df_testa_ferro = (
            socios_de_empresas_ceis.lazy()
            .join(
                df_socios_em_contratos.lazy(),
                on="documento_socio",
                how="inner",
                suffix="_contrato"
            )
            .filter(
                pl.col("cnpj_empresa_ceis") != pl.col("documento_empresa")
            )
            .select([
                pl.col("documento_socio"),
                pl.col("nome_socio"),
                pl.col("cnpj_empresa_ceis"),
                pl.col("nome_empresa_ceis"),
                pl.col("tipo_sancao"),
                pl.col("data_inicio_sancao"),
                pl.col("data_fim_sancao"),
                pl.col("status_sancao"),
                pl.col("documento_empresa").alias("cnpj_empresa_contrato"),
                pl.col("nome_fornecedor").alias("nome_empresa_contrato"),
                pl.col("nome_orgao_comprador"),
                pl.col("valor_global_contrato"),
                pl.col("data_assinatura"),
                pl.col("id_contrato_pncp"),
                pl.lit("TESTA DE FERRO: SOCIO DE EMPRESA IMPEDIDA CONTRATANDO VIA OUTRA EMPRESA")
                    .alias("classificacao_risco"),
            ])
            .collect()
        )

        if df_testa_ferro.height == 0:
            logger.info("Nenhum padrao de testa de ferro detectado nesta amostragem.")
            storage.upload_or_fallback(pl.DataFrame(), GOLD_TESTA_FERRO_PATH)
            return

        storage.upload_or_fallback(df_testa_ferro, GOLD_TESTA_FERRO_PATH)

        logger.info(f"PADROES DE TESTA DE FERRO DETECTADOS: {df_testa_ferro.height}")
        for row in df_testa_ferro.iter_rows(named=True):
            logger.warning(
                f"SOCIO: {row['nome_socio']} | "
                f"Empresa impedida: {row['nome_empresa_ceis']} | "
                f"Contratando via: {row['nome_empresa_contrato']} | "
                f"Orgao: {row['nome_orgao_comprador']} | "
                f"Valor: R$ {row['valor_global_contrato']:,.2f}"
            )


if __name__ == "__main__":
    TestaFerroGoldPipeline().run()