import os
import polars as pl
from src.core.config import settings

input_path = os.path.join(settings.LOCAL_DATA_DIR, "bronze", "ceis_raw.parquet")

if not os.path.exists(input_path):
    print("Arquivo Bronze não encontrado!")
else:
    # Carrega apenas a primeira linha para inspecionar
    df = pl.read_parquet(input_path, n_rows=1)
    
    print("--- SCHEMA DO POLARS ---")
    for col, dtype in df.schema.items():
        print(f"{col}: {dtype}")
        
    print("\n--- CONTEÚDO COMPLETO DA PRIMEIRA LINHA ---")
    # Configura o Polars para não truncar o print do dicionário/struct
    with pl.Config(fmt_str_lengths=1000, tbl_rows=1):
        print(df.to_init_repr())