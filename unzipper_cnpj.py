# unzipper_cnpj.py - FINAL (Com instalação forçada e correção do tqdm)

import os
import re
import zipfile
import shutil 
import sys 
import subprocess

# ==============================================================================
# 🎯 BLOCO DE INSTALAÇÃO FORÇADA DE DEPENDÊNCIAS
# Isso garante que pandas e tqdm estejam disponíveis, mesmo com problemas no venv.
# ==============================================================================
try:
    # Importação corrigida para evitar o erro "'module' object is not callable"
    from tqdm import tqdm as barra_progresso 
    import pandas as pd
except ImportError:
    print("\n" + "=" * 70)
    print("⚠️ DEPENDÊNCIAS FALTANDO! TENTANDO INSTALAÇÃO FORÇADA...")
    print("=" * 70)
    
    comando_pip = [sys.executable, "-m", "pip", "install", "pandas", "tqdm"]
    
    try:
        subprocess.run(comando_pip, check=True, capture_output=True, text=True)
        print("✅ Instalação concluída com sucesso. Tentando carregar novamente...")
        
        # Tenta a importação novamente após a instalação
        from tqdm import tqdm as barra_progresso
        import pandas as pd
        
    except subprocess.CalledProcessError as e:
        print("\n" + "!" * 70)
        print("❌ ERRO CRÍTICO NA INSTALAÇÃO FORÇADA!")
        print("Verifique se o 'pip' está acessível no seu ambiente.")
        print("!" * 70)
        sys.exit(1)
    except ImportError:
        print("❌ ERRO: Bibliotecas ainda não encontradas após instalação. Abortando.")
        sys.exit(1)


# --- Configurações Fixas ---
DIRETORIO_BASE = 'Dados_CNPJ'
DIRETORIO_TRABALHO_NOME = 'Temp_brutos' 
ENCODING_LEITURA = 'iso-8859-1' 
DELIMITADOR_LEITURA = ';'

# ==============================================================================
# CLASSE PRINCIPAL PARA GERENCIAR ESTADO E DIRETÓRIOS
# ==============================================================================

class ProcessadorCNPJ:
    def __init__(self):
        self.diretorio_periodo = self._encontrar_diretorio_mais_recente(DIRETORIO_BASE)
        if not self.diretorio_periodo:
            self.arquivos_zip = []
            return
            
        self.diretorio_saida_trabalho = os.path.join(self.diretorio_periodo, DIRETORIO_TRABALHO_NOME)
        
        try:
            self.arquivos_zip = [f for f in os.listdir(self.diretorio_periodo) if f.lower().endswith('.zip')]
            if not self.arquivos_zip:
                print(f"AVISO: Nenhum arquivo .zip encontrado em {self.diretorio_periodo} para processar.")
        except FileNotFoundError:
            self.arquivos_zip = []

    # --- Funções de Utilitários ---

    def _encontrar_diretorio_mais_recente(self, diretorio_raiz):
        """Localiza a subpasta de período (AAAA-MM) mais recente."""
        try:
            itens = os.listdir(diretorio_raiz)
            padrao_data = re.compile(r'^\d{4}-\d{2}$')
            diretorios_de_periodo = [item for item in itens if os.path.isdir(os.path.join(diretorio_raiz, item)) and padrao_data.match(item)]
            if not diretorios_de_periodo: return None
            diretorio_recente_nome = sorted(diretorios_de_periodo, reverse=True)[0]
            return os.path.join(diretorio_raiz, diretorio_recente_nome)
        except Exception:
            return None

    def _verificar_estado_fases_2_3(self):
        """Verifica se TODAS as subpastas (uma para cada ZIP) foram criadas em Temp_brutos e não estão vazias."""
        
        if not os.path.exists(self.diretorio_saida_trabalho):
            return False

        pastas_destino_completas = True
        for zip_file in self.arquivos_zip:
            nome_pasta_destino = os.path.splitext(zip_file)[0]
            caminho_pasta = os.path.join(self.diretorio_saida_trabalho, nome_pasta_destino)
            
            if not os.path.isdir(caminho_pasta) or len(os.listdir(caminho_pasta)) == 0:
                pastas_destino_completas = False
                break
                
        return pastas_destino_completas

    # --- FASES PRINCIPAIS ---

    def fase_2_3_descompactar_organizado(self):
        """
        FASES 2 & 3: Cria subpastas (FASE 2) e descompacta (FASE 3) DENTRO de Temp_brutos.
        Retorna True/False para o Orquestrador.
        """
        
        if not self.diretorio_periodo: return False
        
        if self._verificar_estado_fases_2_3():
            print("=" * 100)
            print("ESTADO DETECTADO: Subpastas de organização JÁ ESTÃO COMPLETAS em Temp_brutos.")
            print("PULANDO FASES 2 & 3.")
            print("=" * 100)
            return True

        os.makedirs(self.diretorio_saida_trabalho, exist_ok=True)

        print("=" * 70)
        print("FASES 2 & 3: INICIANDO DESCOMPACTAÇÃO ORGANIZADA")
        print("=" * 70)
        
        total_arquivos = len(self.arquivos_zip)
        sucesso_count = 0
        
        # 🐛 CORREÇÃO AQUI: Usando o alias "barra_progresso"
        for nome_zip in barra_progresso(self.arquivos_zip, desc="Progresso Descompactação", unit="arquivo"):
            nome_pasta_destino = os.path.splitext(nome_zip)[0] 
            caminho_pasta_destino = os.path.join(self.diretorio_saida_trabalho, nome_pasta_destino)
            caminho_zip = os.path.join(self.diretorio_periodo, nome_zip)
            
            # PULA se a subpasta JÁ EXISTE e NÃO está vazia
            if os.path.exists(caminho_pasta_destino) and len(os.listdir(caminho_pasta_destino)) > 0:
                sucesso_count += 1
                continue

            try:
                os.makedirs(caminho_pasta_destino, exist_ok=True)
                with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                    if zip_ref.testzip() is not None:
                        raise zipfile.BadZipFile("Checksum de um ou mais arquivos falhou.")

                    zip_ref.extractall(caminho_pasta_destino)
                    sucesso_count += 1
                
            except (zipfile.BadZipFile, FileNotFoundError, Exception) as e:
                print(f"\n     ERRO FATAL ao descompactar {nome_zip}. Pulando este arquivo. Erro: {e}")
                
                if os.path.exists(caminho_pasta_destino):
                    try:
                        shutil.rmtree(caminho_pasta_destino)
                    except OSError:
                        pass
                
        print("\nDescompactação concluída.") 
        print(f"Total de arquivos ZIP na fonte: {total_arquivos}")
        print(f"Total de arquivos descompactados com sucesso (ou já existentes): {sucesso_count}")
        
        if total_arquivos == 0:
            return True 
            
        porcentagem_sucesso = sucesso_count / total_arquivos
        
        if porcentagem_sucesso >= 0.90:
            print("SUCESSO: Mais de 90% dos arquivos ZIP foram descompactados com sucesso.")
            return True
        else:
            print(f"FALHA: Apenas {sucesso_count}/{total_arquivos} arquivos foram descompactados. Processo abortado para investigação.")
            return False

# ==============================================================================
# FUNÇÃO WRAPPER PARA O ORQUESTRADOR
# ==============================================================================

def executar_unzip():
    """
    Função principal wrapper para o Orquestrador Mestre.
    Retorna True ou False.
    """
    try:
        processador = ProcessadorCNPJ()
        
        if not processador.diretorio_periodo:
             print("ERRO: Não foi possível encontrar a pasta de dados mais recente (AAAA-MM) em Dados_CNPJ.")
             return False
             
        if not processador.arquivos_zip and os.path.exists(processador.diretorio_periodo):
             print(f"AVISO: A pasta {processador.diretorio_periodo} está vazia (sem ZIPs). Pulando descompactação.")
             return True
             
        if not processador.fase_2_3_descompactar_organizado():
            print("FALHA CRÍTICA NA DESCOMPACTAÇÃO.")
            return False

        print("\n" + "=" * 100)
        print("FASE 2/3 (DESCOMPACTAÇÃO) CONCLUÍDA COM SUCESSO.")
        print("Os dados brutos estão em pastas organizadas dentro de 'Temp_brutos'.")
        print("=" * 100)
        return True

    except FileNotFoundError as e:
        print(f"\n--- ERRO FATAL ---\n{e}")
        print("Verifique se os arquivos ZIPs foram baixados (Etapa 1) e estão na pasta correta.")
        return False
    except Exception as e:
        print(f"\n--- ERRO INESPERADO ---\nOcorreu um erro inesperado na fase de Descompactação: {e}")
        return False

if __name__ == '__main__':
    executar_unzip()

    