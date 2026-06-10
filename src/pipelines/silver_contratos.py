import os
import polars as pl
from src.core.config import settings
from src.core.logging import setup_logging, logger

setup_logging()

class ContratosSilverPipeline:
    def __init__(self):
        self.input_path = os.path.join(settings.LOCAL_DATA_DIR, "bronze", "contratos_pncp_raw.parquet")
        self.output_dir = os.path.join(settings.LOCAL_DATA_DIR, "silver")
        os.makedirs(self.output_dir, exist_ok=True)

    def run(self):
        logger.info("Iniciando processamento da camada SILVER para os Contratos do PNCP...")
        
        if not os.path.exists(self.input_path):
            logger.error(f"Arquivo Bronze de contratos não encontrado em: {self.input_path}.")
            return

        # 1. Leitura do LazyFrame
        lf = pl.scan_parquet(self.input_path)

        # 2. Transformações e Limpeza de Schema via Polars Expressions
        lf_clean = lf.with_columns([
            # Limpa o documento do fornecedor (remove pontos, traços, barras se houver)
            pl.col("niFornecedor").str.replace_all(r"[\.\-\/\* ]", "").alias("documento_fornecedor_limpo"),
            
            # Normaliza strings de texto para caixa alta
            pl.col("nomeRazaoSocialFornecedor").str.to_uppercase().str.strip_chars().alias("nome_fornecedor"),
            pl.col("objetoContrato").str.strip_chars().alias("objeto_contrato"),
            
            # Extrai os dados da struct do Órgão Comprador
            pl.col("orgaoEntidade").struct.field("cnpj").str.replace_all(r"[\.\-\/\* ]", "").alias("cnpj_orgao_comprador"),
            pl.col("orgaoEntidade").struct.field("razaoSocial").str.to_uppercase().str.strip_chars().alias("nome_orgao_comprador"),
            
            # Realiza o parse das datas (O PNCP geralmente envia YYYY-MM-DD...)
            pl.col("dataAssinatura").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("data_assinatura"),
            pl.col("dataVigenciaInicio").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("data_vigencia_inicio"),
            pl.col("dataVigenciaFim").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("data_vigencia_fim")
            
        ]).select([
            # Seleciona apenas os atributos analíticos úteis para o cruzamento de auditoria
            pl.col("numeroControlePNCP").alias("id_contrato_pncp"),
            pl.col("documento_fornecedor_limpo"),
            pl.col("tipoPessoa").alias("tipo_pessoa_fornecedor"),
            pl.col("nome_fornecedor"),
            pl.col("cnpj_orgao_comprador"),
            pl.col("nome_orgao_comprador"),
            pl.col("data_assinatura"),
            pl.col("data_vigencia_inicio"),
            pl.col("data_vigencia_fim"),
            pl.col("valorGlobal").alias("valor_global_contrato")
        ])

        # 3. Coleta e Persiste os dados limpos
        df_silver = lf_clean.collect()
        output_path = os.path.join(self.output_dir, "contratos_pncp_clean.parquet")
        df_silver.write_parquet(output_path)
        
        logger.info(f"Camada SILVER de Contratos finalizada! Volume: {df_silver.height} linhas.")
        logger.info(f"Arquivo persistido em: {output_path}")

if __name__ == "__main__":
    pipeline = ContratosSilverPipeline()
    pipeline.run()