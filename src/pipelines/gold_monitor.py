import polars as pl
from datetime import date
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

SILVER_CEIS_PATH = "silver/ceis_clean.parquet"
SILVER_CNEP_PATH = "silver/cnep_clean.parquet"
SILVER_CONTRATOS_PATH = "silver/contratos_pncp_clean.parquet"
SILVER_CNPJ_PATH = "silver/cnpj_empresas_clean.parquet"
SILVER_SOCIOS_PATH = "silver/cnpj_socios_clean.parquet"
GOLD_ALERTAS_PATH = "gold/analytics_alertas_corrupcao.parquet"


class MonitorGoldPipeline:

    def run(self):
        logger.info("Iniciando cruzamento Gold — CEIS + CNEP x Contratos...")

        df_ceis = storage.download_parquet(SILVER_CEIS_PATH)
        df_cnep = storage.download_parquet(SILVER_CNEP_PATH)
        df_contratos = storage.download_parquet(SILVER_CONTRATOS_PATH)

        if df_ceis is None or df_contratos is None:
            logger.error("Silver CEIS ou Contratos nao encontrados no R2.")
            return

        if df_cnep is None:
            logger.warning("Silver CNEP nao encontrado — prosseguindo sem CNEP.")

        # --- Cruzamento CEIS x Contratos ---
        df_ceis_matches = (
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
                .alias("classificacao_risco"),
                pl.lit("CEIS").alias("fonte_alerta"),
                pl.lit(None).cast(pl.Float64).alias("valor_multa"),
            ])
            .collect()
        )

        # --- Cruzamento CNEP x Contratos ---
        df_cnep_matches = pl.DataFrame()
        if df_cnep is not None:
            df_cnep_matches = (
                df_contratos.lazy()
                .join(
                    df_cnep.lazy(),
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
                    .then(pl.lit("GRAVISSIMO: EMPRESA PUNIDA POR CORRUPCAO CONTRATANDO"))
                    .otherwise(pl.lit("HISTORICO: FORNECEDOR COM PUNICOES CORRELATAS"))
                    .alias("classificacao_risco"),
                    pl.lit("CNEP").alias("fonte_alerta"),
                    pl.col("valor_multa"),
                ])
                .collect()
            )

        # --- Consolida alertas ---
        colunas_base = [
            "id_contrato_pncp",
            "documento_fornecedor_limpo",
            "nome_fornecedor",
            "nome_orgao_comprador",
            "valor_global_contrato",
            "data_assinatura",
            "nome_empresa_ou_pessoa",
            "tipo_sancao",
            "data_inicio",
            "data_fim",
            "orgao_sancionador",
            "numero_processo",
            "classificacao_risco",
            "fonte_alerta",
            "valor_multa",
        ]

        frames = [df_ceis_matches]
        if df_cnep_matches.height > 0:
            frames.append(df_cnep_matches)

        df_alerts = (
            pl.concat([f.select(colunas_base) for f in frames])
            .sort("classificacao_risco")
        )

        # --- Enriquece com dados do CNPJ ---
        df_empresas = storage.download_parquet(SILVER_CNPJ_PATH)
        if df_empresas is not None:
            df_alerts = df_alerts.join(
                df_empresas.select([
                    "documento_limpo",
                    "situacao_cadastral",
                    "data_inicio_atividade",
                    "capital_social",
                    "porte",
                    "municipio",
                    "uf",
                ]),
                left_on="documento_fornecedor_limpo",
                right_on="documento_limpo",
                how="left"
            )
            logger.info("Alertas enriquecidos com dados do CNPJ.")

        # --- Enriquece com socios ---
        df_socios = storage.download_parquet(SILVER_SOCIOS_PATH)
        if df_socios is not None:
            df_socios_agg = (
                df_socios
                .group_by("documento_empresa")
                .agg(
                    pl.col("nome_socio").str.concat(" | ").alias("socios")
                )
            )
            df_alerts = df_alerts.join(
                df_socios_agg,
                left_on="documento_fornecedor_limpo",
                right_on="documento_empresa",
                how="left"
            )
            logger.info("Alertas enriquecidos com socios.")

        storage.upload_or_fallback(df_alerts, GOLD_ALERTAS_PATH)

        # --- Sumario ---
        total = df_alerts.height
        gravissimos = df_alerts.filter(pl.col("classificacao_risco").str.contains("GRAVISSIMO")).height
        criticos = df_alerts.filter(pl.col("classificacao_risco").str.contains("CRITICO")).height
        historicos = df_alerts.filter(pl.col("classificacao_risco").str.contains("HISTORICO")).height

        logger.info(f"Gold Monitor concluido — {total} alertas gerados.")
        logger.info(f"  GRAVISSIMO : {gravissimos}")
        logger.info(f"  CRITICO    : {criticos}")
        logger.info(f"  HISTORICO  : {historicos}")

        if gravissimos > 0:
            logger.warning(f"ALERTA MAXIMO: {gravissimos} contratos com empresas punidas por corrupcao!")
        if criticos > 0:
            logger.warning(f"ALERTA: {criticos} contratos com empresas impedidas!")


if __name__ == "__main__":
    MonitorGoldPipeline().run()