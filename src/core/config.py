import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # API GovBR / CGU
    CGU_API_BASE_URL: str = "https://api.portaldatransparencia.gov.br/api-v1"
    CGU_API_KEY: str = os.getenv("CGU_API_KEY", "ad62f7c9bd79569e82165448ce33496e")
    
    # Azure Storage / Data Lake
    AZURE_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    AZURE_BRONZE_CONTAINER: str = "bronze"
    
    # Configurações Locais de Fallback
    LOCAL_DATA_DIR: str = "data"

    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8", 
        extra="ignore"
    )

settings = Settings()
