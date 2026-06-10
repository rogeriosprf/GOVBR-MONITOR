import os
import polars as pl
from src.core.config import settings
from src.core.logging import setup_logging, logger

setup_logging()

class MonitorGoldPipeline:
    def __init__(self):
        self.ceis_path = os.path.join(settings.LOCAL_DATA_DIR, "silver", "ceis_clean.parquet")
        self.contratos_path = os.path.join(settings.LOCAL_DATA_DIR, "silver", "contratos_pncp_clean.parquet")
        self.output_dir = os.path.join(settings.LOCAL_DATA_DIR, "gold")
        os.makedirs(self.output_dir, exist_ok=True)

    def run(self):
        logger.info("🕵️‍♂️ Iniciando cruzamento de dados na camada GOLD para detecção de irregularidades...")
        
        if not os.path.exists(self.ceis_path) or not os.path.exists(self.contratos_path):
            logger.error("Arquivos da camada Silver necessários não foram encontrados. Certifique-se de rodar os dois pipelines.")
            return

        # 1. Escaneia os dois Parquets usando LazyFrames (Performance Máxima)
        lf_ceis = pl.scan_parquet(self.ceis_path)
        lf_contratos = pl.scan_parquet(self.contratos_path)

        # 2. Executa o JOIN via Chave de Documento (CPF/CNPJ)
        # Traz apenas os contratos cujos fornecedores possuem alguma sanção ativa/histórica no CEIS
        lf_matches = lf_contratos.join(
            lf_ceis,
            left_on="documento_fornecedor_limpo",
            right_on="documento_limpo",
            how="inner"
        )

        # 3. Aplica a Regra de Negócio de Auditoria (Verificação de Vigência)
        lf_alerts = lf_matches.with_columns([
            pl.when(
                (pl.col("data_assinatura") >= pl.col("data_inicio")) & 
                ((pl.col("data_fim").is_null()) | (pl.col("data_assinatura") <= pl.col("data_fim")))
            )
            .then(pl.lit("CRÍTICO: CONTRATAÇÃO DE EMPRESA IMPEDIDA"))
            .otherwise(pl.lit("HISTÓRICO: FORNECEDOR COM SANÇÕES CORRELATAS"))
            .alias("classificacao_risco")
        ])

        # 4. Computa o plano de execução e coleta o DataFrame
        df_alerts = lf_alerts.collect()

        # 5. Salva a tabela de Alertas Gerados na Gold
        output_path = os.path.join(self.output_dir, "analytics_alertas_corrupcao.parquet")
        df_alerts.write_parquet(output_path)

        logger.info(f"Processamento concluído!")
        logger.info(f"📊 Total de matches encontrados entre Contratos e Empresas Sancionadas: {df_alerts.height}")
        
        # Se houver alertas críticos, exibe o sumário no log
        if df_alerts.height > 0:
            criticos = df_alerts.filter(pl.col("classificacao_risco").str.contains("CRÍTICO")).height
            logger.warning(f"🚨 ALERTA: Detectados {criticos} contratos com indícios de irregularidade grave (empresa impedida)!")
        else:
            logger.info("✨ Nenhum alerta crítico gerado para esta amostragem de dados.")

if __name__ == "__main__":
    pipeline = MonitorGoldPipeline()
    pipeline.run()