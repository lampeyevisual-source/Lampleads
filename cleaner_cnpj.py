# cleaner_cnpj.py - Fase 6: Limpeza Seletiva de ZIPs (CÓDIGO FINAL E CORRIGIDO)

import os
import re
import sys
import subprocess
import shutil # Importado para uso futuro ou potencial limpeza de pasta, embora não usado na fase 6
from typing import List # Usado para tipagem (melhora o Pylance)

# --- Configurações Fixas ---
DIRETORIO_BASE = 'Dados_CNPJ'

# --- Bloco de Instalação/Importação para garantir o tqdm ---
try:
    from tqdm import tqdm as barra_progresso
except ImportError:
    try:
        print("Instalando 'tqdm' para barras de progresso (Fase 6)...")
        # Instala 'tqdm'
        subprocess.run([sys.executable, "-m", "pip", "install", "tqdm"], check=True, capture_output=True)
        from tqdm import tqdm as barra_progresso
    except Exception:
        print("Aviso: 'tqdm' não foi instalado. Usando loop simples.")
        # Fallback simples
        def barra_progresso(iterable, **kwargs):
            return iterable

# ==============================================================================
# CLASSE PRINCIPAL PARA GERENCIAR A LIMPEZA
# ==============================================================================

class ProcessadorLimpeza:
    def __init__(self):
        self.diretorio_periodo: str = self._encontrar_diretorio_mais_recente(DIRETORIO_BASE)
        self.arquivos_zip: List[str] = []
        
        if self.diretorio_periodo and os.path.exists(self.diretorio_periodo):
            try:
                # Lista todos os arquivos ZIPs no diretório de período
                self.arquivos_zip = [f for f in os.listdir(self.diretorio_periodo) if f.lower().endswith('.zip')]
            except FileNotFoundError:
                # Se a pasta sumir, a lista fica vazia.
                pass
        
    def _encontrar_diretorio_mais_recente(self, diretorio_raiz: str) -> str | None:
        """Localiza a subpasta de período (AAAA-MM) mais recente."""
        try:
            itens = os.listdir(diretorio_raiz)
            padrao_data = re.compile(r'^\d{4}-\d{2}$')
            diretorios_de_periodo = [
                item for item in itens 
                if os.path.isdir(os.path.join(diretorio_raiz, item)) and padrao_data.match(item)
            ]
            
            if not diretorios_de_periodo: 
                return None
                
            diretorio_recente_nome = sorted(diretorios_de_periodo, reverse=True)[0] 
            return os.path.join(diretorio_raiz, diretorio_recente_nome)
        except Exception:
            return None

    def fase_6_limpar_arquivos_zip(self) -> bool:
        """
        FASE 6: Remove apenas os arquivos ZIPs do diretório de período.
        (IDEMPOTENTE: Pula se não houver ZIPs para remover).
        """
        
        if not self.diretorio_periodo:
             print("ERRO: Não foi possível encontrar a pasta de dados mais recente (AAAA-MM).")
             return False

        # ======================================================================
        # 🎯 VERIFICAÇÃO DE IDEMPOTÊNCIA: Pula se não há trabalho a fazer.
        # ======================================================================
        if not self.arquivos_zip:
             print("\n" + "=" * 100)
             print("ESTADO DETECTADO: Nenhum arquivo ZIP foi encontrado para remover. (Trabalho concluído).")
             print("PULANDO FASE 6 (LIMPEZA).")
             print("=" * 100)
             return True
             
        # Se há arquivos para limpar, o processamento real começa aqui:
        print("\n" + "=" * 80)
        print("FASE 6: INICIANDO LIMPEZA SELETIVA (REMOVENDO APENAS ARQUIVOS ZIP)")
        print(f"Total de ZIPs encontrados: {len(self.arquivos_zip)}")
        print("================================================================================\n")
        
        sucesso_count = 0
        total_arquivos = len(self.arquivos_zip)
        
        # Inicia a limpeza usando a barra de progresso
        for nome_zip in barra_progresso(self.arquivos_zip, desc="Progresso Limpeza", unit="zip"):
            caminho_zip = os.path.join(self.diretorio_periodo, nome_zip)
            
            try:
                os.remove(caminho_zip)
                sucesso_count += 1
            except Exception as e:
                # Este erro pode ocorrer se o arquivo estiver em uso, por exemplo.
                print(f"\n     ERRO ao remover o arquivo {nome_zip}. Verifique as permissões. Erro: {e}")

        print("\nLimpeza de ZIPs concluída.")
        print(f"Total de ZIPs removidos: {sucesso_count} de {total_arquivos}")
        
        if sucesso_count == total_arquivos:
             print("✅ LIMPEZA CONCLUÍDA! Todos os ZIPs foram removidos.")
             return True
        else:
             print("⚠️ ALERTA: Nem todos os ZIPs foram removidos.")
             return False


# ==============================================================================
# FUNÇÃO WRAPPER PARA O ORQUESTRADOR
# ==============================================================================

def executar_limpeza_zip() -> bool:
    """
    Função principal wrapper para a Limpeza Mestra (Fase 6).
    Retorna True ou False.
    """
    try:
        processador = ProcessadorLimpeza()
        
        if not processador.fase_6_limpar_arquivos_zip():
            print("FALHA CRÍTICA NA LIMPEZA DE ZIPs.")
            return False

        print("\n" + "=" * 100)
        print("FASE 6 (LIMPEZA SELETIVA) CONCLUÍDA COM SUCESSO.")
        print("Os arquivos ZIPs foram removidos.")
        print("=" * 100)
        return True

    except Exception as e:
        print(f"\n--- ERRO INESPERADO ---\nOcorreu um erro inesperado na fase de Limpeza: {e}")
        return False


if __name__ == '__main__':
    executar_limpeza_zip()