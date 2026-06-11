import polars as pl
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

SILVER_TSE_PATH = "silver/tse_receitas_clean.parquet"
SILVER_CONTRATOS_PATH = "silver/contratos_pncp_clean.parquet"
GOLD_TSE_PATH = "gold/analytics_tse_influencia.parquet"


class TseGoldPipeline:

    def run(self):
        logger.info("Iniciando cruzamento Gold TSE — influencia politica x contratos...")

        df_tse = storage.download_parquet(SILVER_TSE_PATH)
        df_contratos = storage.download_parquet(SILVER_CONTRATOS_PATH)

        if df_tse is None or df_contratos is None:
            logger.error("Dados necessarios nao encontrados no R2.")
            return

        # --- Cruzamento principal ---
        # Empresa doou para candidato → essa empresa ganhou contratos públicos
        df_influencia = (
            df_contratos.lazy()
            .join(
                df_tse.lazy(),
                left_on="documento_fornecedor_limpo",
                right_on="documento_doador",
                how="inner"
            )
            .select([
                pl.col("documento_fornecedor_limpo"),
                pl.col("nome_fornecedor"),
                pl.col("nome_orgao_comprador"),
                pl.col("valor_global_contrato"),
                pl.col("data_assinatura"),
                pl.col("nome_candidato"),
                pl.col("partido"),
                pl.col("cargo_candidato"),
                pl.col("uf_candidato"),
                pl.col("municipio_candidato"),
                pl.col("data_doacao"),
                pl.col("valor_doacao"),
                pl.col("numero_recibo"),
                pl.lit("INFLUENCIA POLITICA: DOADOR GANHOU CONTRATO PUBLICO")
                    .alias("classificacao_risco"),
            ])
            .collect()
        )

        if df_influencia.height == 0:
            logger.info("Nenhum cruzamento TSE x Contratos encontrado nesta amostragem.")
            storage.upload_or_fallback(pl.DataFrame(), GOLD_TSE_PATH)
            return

        storage.upload_or_fallback(df_influencia, GOLD_TSE_PATH)
        logger.info(f"TSE Gold concluido — {df_influencia.height} matches de influencia politica!")

        # --- Sumario por candidato ---
        df_por_candidato = (
            df_influencia
            .group_by(["nome_candidato", "partido", "cargo_candidato", "uf_candidato"])
            .agg([
                pl.len().alias("total_contratos_doadores"),
                pl.col("valor_global_contrato").sum().alias("valor_total_contratos"),
                pl.col("valor_doacao").sum().alias("valor_total_doacoes"),
                pl.col("nome_fornecedor").n_unique().alias("empresas_doadoras"),
            ])
            .sort("valor_total_contratos", descending=True)
        )

        logger.info("--- CANDIDATOS COM DOADORES QUE GANHARAM CONTRATOS ---")
        for row in df_por_candidato.head(5).iter_rows(named=True):
            logger.warning(
                f"{row['nome_candidato']} ({row['partido']}/{row['uf_candidato']}) | "
                f"{row['empresas_doadoras']} empresas | "
                f"Doacoes: R$ {row['valor_total_doacoes']:,.2f} | "
                f"Contratos: R$ {row['valor_total_contratos']:,.2f}"
            )


if __name__ == "__main__":
    TseGoldPipeline().run()