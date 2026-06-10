from datetime import date
import polars as pl
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

SILVER_PATH = "silver/ceis_clean.parquet"
GOLD_FACT_PATH = "gold/fact_ceis_sancoes.parquet"
GOLD_ORGAOS_PATH = "gold/dm_sancoes_por_orgao.parquet"


class CeisGoldPipeline:

    def __init__(self):
        self.today = date.today()

    def run(self):
        logger.info("Iniciando Gold CEIS...")

        df_silver = storage.download_parquet(SILVER_PATH)
        if df_silver is None:
            logger.error("Silver CEIS não encontrado no R2.")
            return

        df_gold = df_silver.lazy().with_columns([
            pl.when(
                (pl.col("data_inicio") <= self.today) &
                ((pl.col("data_fim").is_null()) | (pl.col("data_fim") >= self.today))
            )
            .then(pl.lit("ATIVA"))
            .otherwise(pl.lit("EXPIRADA"))
            .alias("status_sancao")
        ]).collect()

        storage.upload_or_fallback(df_gold, GOLD_FACT_PATH)

        df_orgaos = (
            df_gold.group_by("orgao_sancionador")
            .agg([
                pl.len().alias("total_sancoes"),
                pl.col("status_sancao")
                    .filter(pl.col("status_sancao") == "ATIVA")
                    .count()
                    .alias("sancoes_ativas")
            ])
            .sort("total_sancoes", descending=True)
        )

        storage.upload_or_fallback(df_orgaos, GOLD_ORGAOS_PATH)

        total = df_gold.height
        ativas = df_gold.filter(pl.col("status_sancao") == "ATIVA").height
        logger.info(f"Gold CEIS — Total: {total} | Ativas: {ativas} | Expiradas: {total - ativas}")


if __name__ == "__main__":
    CeisGoldPipeline().run()