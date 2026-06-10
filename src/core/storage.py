import boto3
from botocore.exceptions import ClientError
from pathlib import Path
import polars as pl
import io
import logging

from src.core.config import settings

logger = logging.getLogger(__name__)


class R2Storage:
    """Cliente para Cloudflare R2 — compatível com S3."""

    def __init__(self):
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.r2_endpoint_url,
            aws_access_key_id=settings.CLOUDFLARE_ACCESS_KEY_ID,
            aws_secret_access_key=settings.CLOUDFLARE_SECRET_ACCESS_KEY,
            region_name="auto",
        )
        self.bucket = settings.CLOUDFLARE_BUCKET_NAME

    def upload_parquet(self, df: pl.DataFrame, path: str) -> bool:
        """Salva um DataFrame Polars como Parquet no R2."""
        try:
            buffer = io.BytesIO()
            df.write_parquet(buffer, compression="zstd")
            buffer.seek(0)
            self.client.put_object(
                Bucket=self.bucket,
                Key=path,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream",
            )
            logger.info(f"Upload concluído: {path}")
            return True
        except ClientError as e:
            logger.error(f"Erro ao fazer upload para R2: {e}")
            return False

    def download_parquet(self, path: str) -> pl.DataFrame | None:
        """Lê um Parquet do R2 e retorna como DataFrame Polars."""
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=path)
            buffer = io.BytesIO(response["Body"].read())
            return pl.read_parquet(buffer)
        except ClientError as e:
            logger.error(f"Erro ao baixar do R2: {path} — {e}")
            return None

    def file_exists(self, path: str) -> bool:
        """Verifica se um arquivo existe no R2."""
        try:
            self.client.head_object(Bucket=self.bucket, Key=path)
            return True
        except ClientError:
            return False

    def list_files(self, prefix: str) -> list[str]:
        """Lista arquivos no R2 com um prefixo."""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
            )
            return [obj["Key"] for obj in response.get("Contents", [])]
        except ClientError as e:
            logger.error(f"Erro ao listar arquivos no R2: {e}")
            return []

    def save_local_fallback(self, df: pl.DataFrame, path: str) -> bool:
        """Fallback local caso R2 não esteja disponível."""
        try:
            local_path = Path(settings.LOCAL_DATA_DIR) / path
            local_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(local_path, compression="zstd")
            logger.info(f"Salvo localmente: {local_path}")
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar localmente: {e}")
            return False

    def upload_or_fallback(self, df: pl.DataFrame, path: str) -> bool:
        """Tenta R2 primeiro, cai para local se falhar."""
        if self.upload_parquet(df, path):
            return True
        logger.warning(f"R2 indisponível, salvando localmente: {path}")
        return self.save_local_fallback(df, path)


# Instância global
storage = R2Storage()