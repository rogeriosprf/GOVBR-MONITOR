import os
import polars as pl
from src.core.config import settings

input_path = os.path.join(settings.LOCAL_DATA_DIR, "silver", "ceis_clean.parquet")

if not os.path.exists(input_path):
    print("Arquivo Silver não encontrado!")
else:
    # Lendo o arquivo Parquet tratado
    df = pl.read_parquet(input_path)
    
    print("--- SCHEMA DA CAMADA SILVER ---")
    print(df.schema)
    
    print("\n--- PRIMEIROS 5 REGISTROS LIMPOS ---")
    # Configuração para o Polars mostrar todas as colunas sem cortar o texto
    with pl.Config(tbl_cols=10, tbl_rows=5, fmt_str_lengths=50):
        print(df.head(5))