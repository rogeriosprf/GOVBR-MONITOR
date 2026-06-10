import polars as pl
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

SILVER_CEIS_PATH = "silver/ceis_clean.parquet"
SILVER_CONTRATOS_PATH = "silver/contratos_pncp_clean.parquet"
GOLD_ALERTAS_PATH = "gold/analytics_alertas_corrupcao.parquet"


class MonitorGoldPipeline:

    def run(self):
        logger.info("Iniciando cruzamento Gold — detecção de irregularidades...")

        df_ceis = storage.download_parquet(SILVER_CEIS_PATH)
        df_contratos = storage.download_parquet(SILVER_CONTRATOS_PATH)

        if df_ceis is None or df_contratos is None:
            logger.error("Silver CEIS ou Contratos não encontrados no R2.")
            return

        df_alerts = (
            df_contratos.lazy()
            .join(
                df_ceis.lazy(),
                left_on="documento_fornecedor_limpo",
                right_on="documento_limpo",
                how="inner"
            )
            .with_columns([
                pl.when(
                    (pl.col("data_assinatura") >= pl.col("data_inicio")) &
                    (
                        (pl.col("data_fim").is_null()) |
                        (pl.col("data_assinatura") <= pl.col("data_fim"))
                    )
                )
                .then(pl.lit("CRITICO: CONTRATACAO DE EMPRESA IMPEDIDA"))
                .otherwise(pl.lit("HISTORICO: FORNECEDOR COM SANCOES CORRELATAS"))
                .alias("classificacao_risco")
            ])
            .collect()
        )

        storage.upload_or_fallback(df_alerts, GOLD_ALERTAS_PATH)

        total = df_alerts.height
        criticos = df_alerts.filter(
            pl.col("classificacao_risco").str.contains("CRITICO")
        ).height

        logger.info(f"Gold Monitor — {total} matches encontrados.")
        if criticos > 0:
            logger.warning(f"ALERTA: {criticos} contratos com indícios de irregularidade grave!")
        else:
            logger.info("Nenhum alerta crítico para esta amostragem.")


if __name__ == "__main__":
    MonitorGoldPipeline().run()