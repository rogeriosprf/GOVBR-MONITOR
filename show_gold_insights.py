import os
import polars as pl
from src.core.config import settings

mart_path = os.path.join(settings.LOCAL_DATA_DIR, "gold", "dm_sancoes_por_orgao.parquet")

if os.path.exists(mart_path):
    df_orgaos = pl.read_parquet(mart_path)
    print("\n🏛️  TOP 5 ÓRGÃOS SANCIONADORES NO SEU DATA LAKE:")
    with pl.Config(tbl_cols=3, tbl_rows=5, fmt_str_lengths=60):
        print(df_orgaos.head(5))
else:
    print("Data Mart não encontrado!")