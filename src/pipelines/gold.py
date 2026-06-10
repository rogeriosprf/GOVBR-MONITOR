import os
from datetime import date
import polars as pl
from src.core.config import settings
from src.core.logging import setup_logging, logger

setup_logging()

class CeisGoldPipeline:
    def __init__(self):
        self.input_path = os.path.join(settings.LOCAL_DATA_DIR, "silver", "ceis_clean.parquet")
        self.output_dir = os.path.join(settings.LOCAL_DATA_DIR, "gold")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Define a data atual de referência do sistema (2026)
        self.today = date(2026, 6, 9)

    def run(self):
        logger.info("Iniciando processamento da camada GOLD (Métricas Analíticas)...")
        
        if not os.path.exists(self.input_path):
            logger.error(f"Arquivo Silver não encontrado em: {self.input_path}.")
            return

        # 1. Carrega os dados da Silver usando LazyFrame
        lf = pl.scan_parquet(self.input_path)

        # 2. Adiciona Regra de Negócio: Flag de Status Ativo
        # Uma sanção está ATIVA se a data atual está entre o início e o fim da sanção,
        # ou se a data de fim for nula (indeterminada).
        lf_enriched = lf.with_columns([
            pl.when(
                (pl.col("data_inicio") <= self.today) & 
                ((pl.col("data_fim").is_null()) | (pl.col("data_fim") >= self.today))
            )
            .then(pl.lit("ATIVA"))
            .otherwise(pl.lit("EXPIRADA"))
            .alias("status_sancao")
        ])

        # 3. Executa o plano e coleta a tabela principal enriquecida
        df_gold_principal = lf_enriched.collect()
        
        # Salva o arquivo principal da Gold
        main_output = os.path.join(self.output_dir, "fact_ceis_sancoes.parquet")
        df_gold_principal.write_parquet(main_output)
        logger.info(f"Fato principal persistida na Gold: {main_output}")

        # 4. Cria agregações analíticas (Data Marts Rápidos) para o Dashboard
        
        # Agregação A: Resumo por Órgão Sancionador
        df_orgaos = (
            df_gold_principal.group_by("orgao_sancionador")
            .agg([
                pl.len().alias("total_sancoes"),
                pl.col("status_sancao").filter(pl.col("status_sancao") == "ATIVA").count().alias("sancoes_ativas")
            ])
            .sort("total_sancoes", descending=True)
        )
        orgaos_output = os.path.join(self.output_dir, "dm_sancoes_por_orgao.parquet")
        df_orgaos.write_parquet(orgaos_output)

        # Agregação B: Visão Geral de Métricas (KPIs Rápidos)
        total_registros = df_gold_principal.height
        ativas = df_gold_principal.filter(pl.col("status_sancao") == "ATIVA").height
        expiradas = total_registros - ativas
        
        logger.info("--- METRICAS CONSOLIDADAS (GOLD) ---")
        logger.info(f"Total de Sanções Históricas Analisadas: {total_registros}")
        logger.info(f"Sanções Ativas Vigentes: {ativas}")
        logger.info(f"Sanções Já Expiradas: {expiradas}")
        logger.info("Camada GOLD finalizada com sucesso!")

if __name__ == "__main__":
    pipeline = CeisGoldPipeline()
    pipeline.run()