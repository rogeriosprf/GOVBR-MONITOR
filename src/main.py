import sys
from src.core.logging import setup_logging, logger
from src.pipelines.ingestion import CeisIngestionPipeline
from src.pipelines.silver import CeisSilverPipeline
from src.pipelines.gold import CeisGoldPipeline
from src.pipelines.ingestion_cnep import CnepIngestionPipeline
from src.pipelines.silver_cnep import CnepSilverPipeline
from src.pipelines.ingestion_contratos import ContratosIngestionPipeline
from src.pipelines.silver_contratos import ContratosSilverPipeline
from src.pipelines.gold_monitor import MonitorGoldPipeline
from src.pipelines.ingestion_tse import TseIngestionPipeline
from src.pipelines.silver_tse import TseSilverPipeline
from src.pipelines.gold_tse import TseGoldPipeline
from src.pipelines.gold_tse import TseGoldPipeline
from src.pipelines.gold_analytics import AnalyticsGoldPipeline

setup_logging()


def main():
    logger.info("=" * 50)
    logger.info("MONITOR DE CORRUPCAO v4.0 — CEIS + CNEP + PNCP + CNPJ + TSE")
    logger.info("=" * 50)

    try:
        logger.info("[1/8] Ingestao CEIS (Bronze)...")
        CeisIngestionPipeline().run(max_pages=30)

        logger.info("[2/8] Tratamento CEIS (Silver)...")
        CeisSilverPipeline().run()

        logger.info("[3/8] Metricas CEIS (Gold)...")
        CeisGoldPipeline().run()

        logger.info("[4/8] Ingestao CNEP (Bronze)...")
        CnepIngestionPipeline().run(max_pages=30)

        logger.info("[5/8] Tratamento CNEP (Silver)...")
        CnepSilverPipeline().run()

        logger.info("[6/8] Ingestao Contratos PNCP (Bronze)...")
        ContratosIngestionPipeline().run(dias=2, max_paginas_por_dia=2)

        logger.info("[7/8] Tratamento Contratos PNCP (Silver)...")
        ContratosSilverPipeline().run()

        logger.info("[8/8] Cruzamento de Inteligencia (Gold)...")
        MonitorGoldPipeline().run()

        logger.info("[9/11] Silver TSE...")
        TseSilverPipeline().run()

        logger.info("[10/11] Cruzamento TSE x Contratos (Gold)...")
        TseGoldPipeline().run()

        logger.info("[11/11] Analytics avancados...")
        AnalyticsGoldPipeline().run()

        logger.info("=" * 50)
        logger.info("MONITOR EXECUTADO COM SUCESSO")
        logger.info("=" * 50)

    except Exception as e:
        logger.critical(f"Falha na orquestracao: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()