import os
import polars as pl
from src.core.config import settings
from src.core.logging import setup_logging, logger

# Inicializa o log para vermos o progresso no terminal
setup_logging()

class CeisSilverPipeline:
    def __init__(self):
        self.input_path = os.path.join(settings.LOCAL_DATA_DIR, "bronze", "ceis_raw.parquet")
        self.output_dir = os.path.join(settings.LOCAL_DATA_DIR, "silver")
        os.makedirs(self.output_dir, exist_ok=True)

    def run(self):
        logger.info("Iniciando processamento da camada SILVER para o CEIS...")
        
        if not os.path.exists(self.input_path):
            logger.error(f"Arquivo Bronze não encontrado em: {self.input_path}. Execute a ingestão primeiro.")
            return

        # 1. Leitura do Parquet bruto com LazyFrame
        lf = pl.scan_parquet(self.input_path)

        # 2. Transformações baseadas no Schema Real da v2
        lf_clean = lf.with_columns([
            # Extrai CPF ou CNPJ de dentro do objeto 'pessoa'
            pl.col("pessoa").struct.field("cpfFormatado").alias("cpf_raw"),
            pl.col("pessoa").struct.field("cnpjFormatado").alias("cnpj_raw"),
            
            # Extrai o Tipo de Pessoa (Física ou Jurídica)
            pl.col("pessoa").struct.field("tipo").alias("tipo_pessoa"),

            # Converte datas de string ('DD/MM/AAAA') para o tipo Date real do Polars
            pl.col("dataInicioSancao").str.to_date("%d/%m/%Y", strict=False),
            pl.col("dataFimSancao").str.to_date("%d/%m/%Y", strict=False),
            
            # Extrai dados textuais das Structs e limpa strings
            pl.col("sancionado").struct.field("nome").str.to_uppercase().str.strip_chars().alias("nome_sancionado"),
            pl.col("tipoSancao").struct.field("descricaoResumida").str.to_uppercase().str.strip_chars().alias("tipo_sancao"),
            pl.col("orgaoSancionador").struct.field("nome").str.to_uppercase().str.strip_chars().alias("orgao_sancionador")
        ]).with_columns([
            # Cria uma única coluna de documento limpo (se cnpj_raw for vazio, usa o cpf_raw)
            pl.coalesce([
                pl.when(pl.col("cnpj_raw") != "").then(pl.col("cnpj_raw")).otherwise(None),
                pl.when(pl.col("cpf_raw") != "").then(pl.col("cpf_raw")).otherwise(None)
            ]).str.replace_all(r"[\.\-\/\*]", "").alias("documento_limpo")
        ]).select([
            # Seleção final limpa para a camada analítica
            pl.col("id").alias("id_sancao"),
            pl.col("documento_limpo"),
            pl.col("tipo_pessoa"),
            pl.col("nome_sancionado").alias("nome_empresa_ou_pessoa"),
            pl.col("tipo_sancao"),
            pl.col("dataInicioSancao").alias("data_inicio"),
            pl.col("dataFimSancao").alias("data_fim"),
            pl.col("orgao_sancionador"),
            pl.col("numeroProcesso").alias("numero_processo")
        ])

        # 3. Coleta e persiste os dados limpos
        df_silver = lf_clean.collect()
        
        output_path = os.path.join(self.output_dir, "ceis_clean.parquet")
        df_silver.write_parquet(output_path)
        
        logger.info(f"Camada SILVER processada com sucesso! Volume: {df_silver.height} linhas.")
        logger.info(f"Arquivo persistido em: {output_path}")

if __name__ == "__main__":
    pipeline = CeisSilverPipeline()
    pipeline.run()