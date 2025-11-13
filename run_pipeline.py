# run_pipeline.py - O Orquestrador Mestre FINAL (Baseado em Importação)

import sys
import time

# ==============================================================================
# 1. IMPORTAÇÃO DOS MÓDULOS DE FASE
# Importa todas as funções principais dos scripts do pipeline.
# ==============================================================================
try:
    # Fases principais
    from downloader_cnpj import executar_download
    from unzipper_cnpj import executar_unzip
    from organizer_cnpj import executar_consolidacao
    
    # 🎯 NOVA FASE: Limpeza Seletiva (cleaner_cnpj.py)
    try:
        from cleaner_cnpj import executar_limpeza_zip
    except ImportError:
        # Fallback: Se o cleaner_cnpj.py não for encontrado, ignora a limpeza.
        print("\nAVISO: O script 'cleaner_cnpj.py' não foi encontrado. A fase 6 de Limpeza de ZIPs será ignorada.")
        def executar_limpeza_zip():
            return True # Retorna sucesso para não parar o pipeline
            
except ImportError as e:
    print("-" * 70)
    print("ERRO DE IMPORTAÇÃO CRÍTICO!")
    print(f"Não foi possível importar um dos módulos do pipeline: {e}")
    print("Verifique se todos os arquivos (.py) estão no mesmo diretório que este script.")
    print("-" * 70)
    sys.exit(1) # Sai do programa se houver erro de importação

# ==============================================================================
# 2. FUNÇÃO AUXILIAR PARA EXECUÇÃO DE FASE
# ==============================================================================

def executar_fase(nome_fase, funcao_fase):
    """Executa uma fase do pipeline, registra o tempo e verifica o status."""
    start_time = time.time()
    
    print("\n" + "=" * 80)
    print(f"🔄 INICIANDO FASE: {nome_fase}")
    print("=" * 80)
    
    # Chama a função principal de cada módulo. Ela deve retornar True ou False.
    sucesso = funcao_fase()
    
    end_time = time.time()
    duracao = end_time - start_time
    
    # Exibição do status da fase
    status_msg = "✅ CONCLUÍDA" if sucesso else "❌ FALHOU"
    print("-" * 80)
    print(f"FASE {nome_fase} {status_msg} em {duracao:.2f} segundos.")
    print("-" * 80)
    
    return sucesso

# ==============================================================================
# 3. FUNÇÃO PRINCIPAL DO PIPELINE
# ==============================================================================

def pipeline_principal():
    """Define e executa a sequência de fases do pipeline ETL (Extrair, Transformar, Carregar/Limpar)."""
    pipeline_start_time = time.time()
    
    print("=" * 80)
    print("INÍCIO DO PIPELINE ETL DE DADOS CNPJ")
    print("================================================================================")
    
    # --- FASE 1: DOWNLOAD ---
    if not executar_fase("1/6 - DOWNLOAD DE ARQUIVOS ZIP", executar_download):
        print("\n🛑 PIPELINE PARADO: A FASE DE DOWNLOAD FALHOU.")
        return 
        
    # --- FASE 2/3: DESCOMPACTAÇÃO E ORGANIZAÇÃO INICIAL ---
    if not executar_fase("2/6 & 3/6 - DESCOMPACTAÇÃO E ORGANIZAÇÃO", executar_unzip):
        print("\n🛑 PIPELINE PARADO: A FASE DE DESCOMPACTAÇÃO FALHOU.")
        return 
        
    # --- FASE 4/5: CONSOLIDAÇÃO E GERAÇÃO DO CSV MESTRE ---
    if not executar_fase("4/6 & 5/6 - CONSOLIDAÇÃO E GERAÇÃO DO CSV MESTRE", executar_consolidacao):
        print("\n🛑 PIPELINE PARADO: A FASE DE CONSOLIDAÇÃO FALHOU.")
        return 
        
    # 🎯 FASE 6: LIMPEZA SELETIVA DE ZIPS
    if not executar_fase("6/6 - LIMPEZA SELETIVA DE ZIPS", executar_limpeza_zip):
        # A falha na limpeza não interrompe o sucesso do pipeline, pois o CSV Mestre já foi gerado.
        print("\n⚠️ AVISO: A FASE DE LIMPEZA FALHOU. O CSV MESTRE foi gerado, mas os ZIPs podem ter permanecido.")
        
    # --- FIM DO PIPELINE ---
    pipeline_end_time = time.time()
    duracao_total = pipeline_end_time - pipeline_start_time
    
    print("\n\n" + "#" * 80)
    print("🎉 PIPELINE ETL CONCLUÍDO COM SUCESSO TOTAL! 🎉")
    print(f"O CSV MESTRE FINAL está pronto. DURAÇÃO TOTAL DO PROCESSO: {duracao_total:.2f} segundos.")
    print("#" * 80)

if __name__ == '__main__':
    pipeline_principal()