import polars as pl
from datetime import date, timedelta
from src.core.logging import setup_logging, logger
from src.core.storage import storage

setup_logging()

SILVER_CONTRATOS_PATH = "silver/contratos_pncp_clean.parquet"
SILVER_CNPJ_PATH = "silver/cnpj_empresas_clean.parquet"
GOLD_ALERTAS_PATH = "gold/analytics_alertas_corrupcao.parquet"
GOLD_FRACIONAMENTO_PATH = "gold/analytics_fracionamento.parquet"
GOLD_CONCENTRACAO_PATH = "gold/analytics_concentracao.parquet"
GOLD_ORGAOS_RISCO_PATH = "gold/analytics_orgaos_risco.parquet"
GOLD_EMPRESAS_NOVAS_PATH = "gold/analytics_empresas_novas.parquet"
GOLD_TEMPORAL_PATH = "gold/analytics_temporal.parquet"
GOLD_EMPENHO_PATH = "gold/analytics_empenho_direto.parquet"


class AnalyticsGoldPipeline:

    def run(self):
        logger.info("Iniciando pipeline de analytics avancados...")

        df_contratos = storage.download_parquet(SILVER_CONTRATOS_PATH)
        df_cnpj = storage.download_parquet(SILVER_CNPJ_PATH)
        df_alertas = storage.download_parquet(GOLD_ALERTAS_PATH)

        if df_contratos is None:
            logger.error("Silver Contratos nao encontrado.")
            return

        # --- 1. Fracionamento ---
        self._detectar_fracionamento(df_contratos)

        # --- 2. Concentração ---
        self._detectar_concentracao(df_contratos)

        # --- 3. Órgãos com maior risco ---
        if df_alertas is not None:
            self._orgaos_risco(df_contratos, df_alertas)

        # --- 4. Empresas novas com contratos ---
        if df_cnpj is not None:
            self._empresas_novas(df_contratos, df_cnpj)

        # --- 5. Evolução temporal ---
        self._evolucao_temporal(df_contratos, df_alertas)

        # --- 6. Empenho direto suspeito ---
        self._empenho_direto(df_contratos)

        logger.info("Analytics avancados concluidos.")

    def _detectar_fracionamento(self, df: pl.DataFrame):
        """Mesmo fornecedor, mesmo órgão, múltiplos contratos pequenos."""
        logger.info("Detectando fracionamento de contratos...")

        # Threshold de dispensa de licitação — R$ 57.208,33
        LIMITE_DISPENSA = 57_208.33

        df_frac = (
            df.group_by(["documento_fornecedor_limpo", "nome_fornecedor", "cnpj_orgao_comprador", "nome_orgao_comprador"])
            .agg([
                pl.len().alias("total_contratos"),
                pl.col("valor_global_contrato").sum().alias("valor_total"),
                pl.col("valor_global_contrato").mean().alias("valor_medio"),
                pl.col("valor_global_contrato").max().alias("valor_maximo"),
                pl.col("data_assinatura").min().alias("primeiro_contrato"),
                pl.col("data_assinatura").max().alias("ultimo_contrato"),
            ])
            .filter(
                (pl.col("total_contratos") >= 3) &
                (pl.col("valor_medio") < LIMITE_DISPENSA) &
                (pl.col("valor_total") > LIMITE_DISPENSA)
            )
            .with_columns([
                pl.lit("SUSPEITA DE FRACIONAMENTO").alias("classificacao"),
                (pl.col("valor_total") / LIMITE_DISPENSA).round(2).alias("multiplos_do_limite"),
            ])
            .sort("total_contratos", descending=True)
        )

        storage.upload_or_fallback(df_frac, GOLD_FRACIONAMENTO_PATH)
        logger.info(f"Fracionamento — {df_frac.height} padroes suspeitos detectados.")
        if df_frac.height > 0:
            logger.warning(f"ALERTA: {df_frac.height} casos suspeitos de fracionamento!")
            for row in df_frac.head(3).iter_rows(named=True):
                logger.warning(
                    f"{row['nome_fornecedor']} → {row['nome_orgao_comprador']} | "
                    f"{row['total_contratos']} contratos | "
                    f"Total: R$ {row['valor_total']:,.2f}"
                )

    def _detectar_concentracao(self, df: pl.DataFrame):
        """Fornecedores dominando contratos em múltiplos órgãos."""
        logger.info("Detectando concentracao de contratos...")

        df_conc = (
            df.group_by(["documento_fornecedor_limpo", "nome_fornecedor"])
            .agg([
                pl.len().alias("total_contratos"),
                pl.col("cnpj_orgao_comprador").n_unique().alias("orgaos_distintos"),
                pl.col("valor_global_contrato").sum().alias("valor_total"),
                pl.col("valor_global_contrato").mean().alias("valor_medio"),
            ])
            .with_columns([
                (pl.col("valor_total") / df["valor_global_contrato"].sum() * 100)
                .round(2).alias("pct_valor_total"),
            ])
            .sort("valor_total", descending=True)
        )

        storage.upload_or_fallback(df_conc, GOLD_CONCENTRACAO_PATH)
        logger.info(f"Concentracao — {df_conc.height} fornecedores mapeados.")

        top = df_conc.head(1).row(0, named=True)
        logger.info(
            f"Maior concentracao: {top['nome_fornecedor']} | "
            f"{top['total_contratos']} contratos | "
            f"R$ {top['valor_total']:,.2f} ({top['pct_valor_total']}% do total)"
        )

    def _orgaos_risco(self, df_contratos: pl.DataFrame, df_alertas: pl.DataFrame):
        logger.info("Calculando risco por orgao...")

        # Usa nome_orgao_comprador já que cnpj não está no Gold
        score_expr = (
            pl.col("score_risco_total").mean().round(1).alias("score_medio")
            if "score_risco_total" in df_alertas.columns
            else pl.lit(0).alias("score_medio")
        )

        df_orgaos = (
        df_alertas
        .group_by("nome_orgao_comprador")
        .agg([
            pl.len().alias("total_alertas"),
            pl.col("classificacao_risco")
                .filter(pl.col("classificacao_risco").str.contains("CRITICO"))
                .count().alias("alertas_criticos"),
            pl.col("valor_global_contrato").sum().alias("valor_total_suspeito"),
            score_expr,
        ])
        .sort("total_alertas", descending=True)
    )

        # Enriquece com total de contratos do órgão
        df_total_orgao = (
            df_contratos
            .group_by("nome_orgao_comprador")
            .agg(pl.len().alias("total_contratos_orgao"))
        )

        df_orgaos = df_orgaos.join(
            df_total_orgao,
            on="nome_orgao_comprador",
            how="left"
        ).with_columns([
        (pl.col("total_alertas") / pl.col("total_contratos_orgao") * 100)
        .round(2).alias("pct_contratos_suspeitos")
    ])

        storage.upload_or_fallback(df_orgaos, GOLD_ORGAOS_RISCO_PATH)
        logger.info(f"Orgaos risco — {df_orgaos.height} orgaos mapeados.")

    def _empresas_novas(self, df_contratos: pl.DataFrame, df_cnpj: pl.DataFrame):
        """Empresas jovens com muitos contratos."""
        logger.info("Mapeando empresas novas com contratos...")

        hoje = date.today()
        um_ano_atras = hoje - timedelta(days=365)

        df_novas = (
            df_cnpj
            .filter(pl.col("data_inicio_atividade") >= um_ano_atras)
            .join(
                df_contratos.group_by("documento_fornecedor_limpo").agg([
                    pl.len().alias("total_contratos"),
                    pl.col("valor_global_contrato").sum().alias("valor_total"),
                    pl.col("nome_orgao_comprador").n_unique().alias("orgaos_distintos"),
                ]),
                left_on="documento_limpo",
                right_on="documento_fornecedor_limpo",
                how="inner"
            )
            .with_columns([
                (
                    (pl.lit(hoje) - pl.col("data_inicio_atividade"))
                    .dt.total_days()
                ).alias("dias_desde_abertura")
            ])
            .sort("valor_total", descending=True)
        )

        storage.upload_or_fallback(df_novas, GOLD_EMPRESAS_NOVAS_PATH)
        logger.info(f"Empresas novas com contratos — {df_novas.height} encontradas.")

        if df_novas.height > 0:
            for row in df_novas.head(3).iter_rows(named=True):
                logger.info(
                    f"{row['razao_social']} | "
                    f"Aberta ha {row['dias_desde_abertura']} dias | "
                    f"{row['total_contratos']} contratos | "
                    f"R$ {row['valor_total']:,.2f}"
                )

    def _evolucao_temporal(self, df_contratos: pl.DataFrame, df_alertas: pl.DataFrame | None):
        """Evolução diária de contratos e alertas."""
        logger.info("Calculando evolucao temporal...")

        df_temporal = (
            df_contratos
            .group_by("data_assinatura")
            .agg([
                pl.len().alias("total_contratos"),
                pl.col("valor_global_contrato").sum().alias("valor_total"),
            ])
            .sort("data_assinatura")
        )

        if df_alertas is not None and "data_assinatura" in df_alertas.columns:
            df_alertas_temporal = (
                df_alertas
                .group_by("data_assinatura")
                .agg(pl.len().alias("total_alertas"))
            )
            df_temporal = df_temporal.join(
                df_alertas_temporal,
                on="data_assinatura",
                how="left"
            ).with_columns(
                pl.col("total_alertas").fill_null(0)
            )

        storage.upload_or_fallback(df_temporal, GOLD_TEMPORAL_PATH)
        logger.info(f"Temporal — {df_temporal.height} dias mapeados.")

    def _empenho_direto(self, df_contratos: pl.DataFrame):
        """Contratos por empenho direto — sem processo formal."""
        logger.info("Mapeando empenhos diretos...")

        # tipoContrato está como struct no bronze mas já foi extraído no silver
        # Verifica se a coluna existe processada
        if "tipo_contrato" not in df_contratos.columns:
            logger.warning("Campo tipo_contrato nao encontrado no Silver — pulando analise de empenho.")
            return

        df_empenho = (
            df_contratos
            .filter(pl.col("tipo_contrato").str.contains("Empenho"))
            .group_by(["documento_fornecedor_limpo", "nome_fornecedor", "nome_orgao_comprador"])
            .agg([
                pl.len().alias("total_empenhos"),
                pl.col("valor_global_contrato").sum().alias("valor_total"),
            ])
            .sort("valor_total", descending=True)
        )

        storage.upload_or_fallback(df_empenho, GOLD_EMPENHO_PATH)
        logger.info(f"Empenhos diretos — {df_empenho.height} padroes mapeados.")


if __name__ == "__main__":
    AnalyticsGoldPipeline().run()