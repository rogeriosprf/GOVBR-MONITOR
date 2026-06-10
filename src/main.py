import sys
from datetime import datetime, timedelta
from src.core.logging import setup_logging, logger
from src.pipelines.ingestion import CeisIngestionPipeline
from src.pipelines.silver import CeisSilverPipeline
from src.pipelines.gold import CeisGoldPipeline
from src.pipelines.ingestion_contratos import ContratosIngestionPipeline
from src.pipelines.silver_contratos import ContratosSilverPipeline
from src.pipelines.gold_monitor import MonitorGoldPipeline

setup_logging()

def main():
    logger.info("==================================================")
    logger.info("🚀 INICIANDO ENGINE DO MONITOR DE CORRUPÇÃO v1.0")
    logger.info("==================================================")
    
    try:
        # ---- ESTEIRA 1: CEIS (Aumentando para 15 páginas de sanções) ----
        logger.info("🔹 [1/6] Ingestão CEIS (Bronze) - Expandindo Amostragem...")
        CeisIngestionPipeline().run(max_pages=15) 
        
        logger.info("🔹 [2/6] Tratamento CEIS (Silver)...")
        CeisSilverPipeline().run()
        
        logger.info("🔹 [3/6] Métricas CEIS (Gold)...")
        CeisGoldPipeline().run()
        
        # ---- ESTEIRA 2: PNCP (Varrendo múltiplos dias de contratos) ----
        logger.info("🔹 [4/6] Ingestão Contratos PNCP (Bronze) - Loop Histórico...")
        
        contratos_pipeline = ContratosIngestionPipeline()
        
        # Vamos gerar uma lista de datas para varrer os últimos 7 dias
        # Mudando a lógica do seu pipeline interno para acumular os dados
        data_base = datetime(2026, 6, 1)
        for i in range(7):
            data_alvo = (data_base + timedelta(days=i)).strftime("%Y%m%dd")
            # Força o download do lote diário (reutilizando a estrutura que criamos)
            contratos_pipeline.fetch_contratos_por_data(data_alvo=data_alvo[:-1], pagina=1)
            
        # Para fins de portfólio e para o teste ganhar corpo, vamos rodar a carga
        # consolidada que já temos expandida de forma nativa.
        contratos_pipeline.run(data_alvo="20260601", max_paginas=5) # Baixando 5 páginas do mesmo dia (250 contratos)
        
        logger.info("🔹 [5/6] Tratamento Contratos PNCP (Silver)...")
        ContratosSilverPipeline().run()
        
        # ---- ESTEIRA 3: MONITOR DE AUDITORIA (O Cruzamento em Larga Escala) ----
        logger.info("🔹 [6/6] Rodando Cruzamento de Inteligência (Gold)...")
        MonitorGoldPipeline().run()

        logger.info("==================================================")
        logger.info("✅ MONITOR DE CORRUPÇÃO EXECUTADO COM SUCESSO!")
        logger.info("==================================================")

    except Exception as e:
        logger.critical(f"💥 Falha catastrófica na orquestração: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()