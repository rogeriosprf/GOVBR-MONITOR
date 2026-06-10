import sys
from src.core.logging import setup_logging, logger
from src.pipelines.ingestion import CeisIngestionPipeline
from src.pipelines.silver import CeisSilverPipeline
from src.pipelines.gold import CeisGoldPipeline
from src.pipelines.ingestion_contratos import ContratosIngestionPipeline
from src.pipelines.silver_contratos import ContratosSilverPipeline
from src.pipelines.gold_monitor import MonitorGoldPipeline

setup_logging()


def main():
    logger.info("=" * 50)
    logger.info("INICIANDO MONITOR DE CORRUPÇÃO v2.0")
    logger.info("=" * 50)

    try:
        logger.info("[1/6] Ingestão CEIS (Bronze)...")
        CeisIngestionPipeline().run(max_pages=15)

        logger.info("[2/6] Tratamento CEIS (Silver)...")
        CeisSilverPipeline().run()

        logger.info("[3/6] Métricas CEIS (Gold)...")
        CeisGoldPipeline().run()

        logger.info("[4/6] Ingestão Contratos PNCP (Bronze)...")
        ContratosIngestionPipeline().run(dias=7, max_paginas_por_dia=5)

        logger.info("[5/6] Tratamento Contratos PNCP (Silver)...")
        ContratosSilverPipeline().run()

        logger.info("[6/6] Cruzamento de Inteligência (Gold)...")
        MonitorGoldPipeline().run()

        logger.info("=" * 50)
        logger.info("MONITOR EXECUTADO COM SUCESSO")
        logger.info("=" * 50)

    except Exception as e:
        logger.critical(f"Falha na orquestração: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()