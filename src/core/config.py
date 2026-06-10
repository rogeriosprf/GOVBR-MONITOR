import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # API GovBR / CGU
    CGU_API_BASE_URL: str = "https://api.portaldatransparencia.gov.br/api-de-dados"
    CGU_API_KEY: str = ""

    # Cloudflare R2
    CLOUDFLARE_ACCOUNT_ID: str = ""
    CLOUDFLARE_ACCESS_KEY_ID: str = ""
    CLOUDFLARE_SECRET_ACCESS_KEY: str = ""
    CLOUDFLARE_BUCKET_NAME: str = "govbr-datalake"

    # Endpoint R2 — gerado automaticamente com base no account_id
    @property
    def r2_endpoint_url(self) -> str:
        return f"https://{self.CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com"

    # Configurações locais de fallback
    LOCAL_DATA_DIR: str = "data"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()