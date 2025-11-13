# organizer_cnpj.py - Versão FINALMENTE Corrigida e Completa com Idempotência

import os
import re
import csv
from tqdm import tqdm
import shutil 
import sys 

# --- Configurações Fixas ---
DIRETORIO_BASE = 'Dados_CNPJ'
DIRETORIO_TRABALHO_NOME = 'Temp_brutos' 
NOME_ARQUIVO_MESTRE = 'CSV_Mestre_Final.csv'

ENCODING_LEITURA = 'iso-8859-1' 
DELIMITADOR_PADRAO = ';' 

# ==============================================================================
# 1. MAPA DE COLUNAS DEFINITIVO (SEU SCHEMA PARA CONSOLIDAÇÃO)
# ==============================================================================

MAPA_COLUNAS_CONSOLIDADO = {
    'CNAES': [(0, 'codigo_cnae'), (1, 'descricao_cnae')],
    'MOTIVOS': [(0, 'codigo_motivo'), (1, 'descricao_motivo')],
    'MUNIC': [(0, 'codigo_municipio'), (1, 'nome_municipio')],
    'NATJU': [(0, 'codigo_natureza_juridica'), (1, 'descricao_natureza_juridica')],
    'PAIS': [(0, 'codigo_pais'), (1, 'descricao_pais')],
    'QUALS': [(0, 'codigo_qualificacao'), (1, 'descricao_qualificacao')],
    'EMPRE': [(0, 'cnpj_basico'), (1, 'razao_social'), (2, 'natureza_juridica'), (3, 'qualificacao_socio_responsavel'), (4, 'capital_social'), (5, 'porte_empresa'), (6, 'ente_federativo_responsavel')],
    'ESTABELE': [(0, 'cnpj_basico'), (1, 'cnpj_ordem'), (2, 'cnpj_dv'), (3, 'matriz_filial'), (4, 'nome_fantasia'), (5, 'situacao_cadastral'), (6, 'data_situacao_cadastral'), (7, 'motivo_situacao_cadastral'), (8, 'nome_cidade_exterior'), (9, 'pais'), (10, 'data_inicio_atividade'), (11, 'cnae_fiscal_principal'), (12, 'cnae_fiscal_secundario'), (13, 'logradouro'), (14, 'numero'), (15, 'complemento'), (16, 'bairro'), (17, 'cep'), (18, 'uf'), (19, 'codigo_municipio'), (20, 'ddd_1'), (21, 'telefone_1'), (22, 'ddd_2'), (23, 'telefone_2'), (24, 'ddd_fax'), (25, 'fax'), (26, 'correio_eletronico'), (27, 'situacao_especial'), (28, 'data_situacao_especial')],
    'SOCIO': [(0, 'cnpj_basico'), (1, 'tipo_socio'), (2, 'nome_socio'), (3, 'cpf_cnpj_socio'), (4, 'qualificacao_socio'), (5, 'data_entrada_sociedade'), (6, 'pais'), (7, 'representante_legal'), (8, 'nome_representante'), (9, 'qualificacao_representante'), (10, 'data_entrada_representante')],
    'SIMPLES': [(0, 'cnpj_basico'), (1, 'opcao_simples'), (2, 'data_opcao_simples'), (3, 'data_exclusao_simples'), (4, 'opcao_mei'), (5, 'data_opcao_mei'), (6, 'data_exclusao_mei')]
}

todos_nomes = set()
for arquivo, mapeamento in MAPA_COLUNAS_CONSOLIDADO.items():
    for _, nome_coluna in mapeamento:
        todos_nomes.add(nome_coluna)

ORDEM_PRIORIDADE = [
    'cnpj_basico', 'razao_social', 'cnpj_ordem', 'cnpj_dv', 
    'matriz_filial', 'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
    'motivo_situacao_cadastral', 'logradouro', 'numero', 'complemento', 'bairro',
    'cep', 'uf', 'codigo_municipio', 
    'ddd_1', 'telefone_1', 'correio_eletronico',
    'capital_social', 'porte_empresa', 'ente_federativo_responsavel', 'data_inicio_atividade',
    'situacao_especial', 'data_situacao_especial', 
    'tipo_socio', 'nome_socio', 'cpf_cnpj_socio', 'qualificacao_socio',
    'data_entrada_sociedade', 'representante_legal', 'nome_representante', 'qualificacao_representante',
    'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei',
    'data_exclusao_mei', 
    'natureza_juridica', 'codigo_natureza_juridica', 'descricao_natureza_juridica',
    'qualificacao_socio_responsavel', 'codigo_qualificacao', 'descricao_qualificacao', 
    'codigo_pais', 'descricao_pais', 'pais', 'nome_cidade_exterior',
    'codigo_cnae', 'descricao_cnae', 'cnae_fiscal_principal', 'cnae_fiscal_secundario',
    'codigo_motivo', 'descricao_motivo',
    'nome_municipio', 
]

CABECALHO_FINAL = [col for col in ORDEM_PRIORIDADE if col in todos_nomes]
CABECALHO_FINAL.append('TABELA_ORIGEM')

# ==============================================================================
# CLASSE PRINCIPAL PARA PROCESSAMENTO (FASES 4 e 5)
# ==============================================================================

class ProcessadorConsolidacaoELimpeza:
    def __init__(self):
        self.diretorio_periodo = self._encontrar_diretorio_mais_recente(DIRETORIO_BASE)
        if not self.diretorio_periodo:
            return
            
        self.diretorio_saida_trabalho = os.path.join(self.diretorio_periodo, DIRETORIO_TRABALHO_NOME)
        self.diretorio_saida_final = self.diretorio_periodo

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
            
    def _detectar_delimitador(self, caminho_arquivo):
        """Tenta detectar o delimitador do arquivo CSV."""
        try:
            with open(caminho_arquivo, 'r', encoding=ENCODING_LEITURA) as f:
                amostra = f.read(1024)
                if not amostra:
                    return DELIMITADOR_PADRAO
                
                dialeto = csv.Sniffer().sniff(amostra, delimiters=';,\t|') 
                return dialeto.delimiter
        except Exception:
            return DELIMITADOR_PADRAO

    def fase_4_5_consolidar_csv_mestre(self):
        """FASE 4/5: Transforma, limpa e consolida todos os dados em UM ÚNICO CSV MESTRE."""
        
        caminho_saida_final = os.path.join(self.diretorio_saida_final, NOME_ARQUIVO_MESTRE)
        
        # ======================================================================
        # 🎯 VERIFICAÇÃO DE IDEMPOTÊNCIA (CORREÇÃO PEDIDA)
        # ======================================================================
        # Verifica se o arquivo final existe e tem um tamanho razoável (acima de 1MB)
        if os.path.exists(caminho_saida_final) and os.path.getsize(caminho_saida_final) > 1024 * 1024:
            print("\n" + "=" * 100)
            print("ESTADO DETECTADO: CSV_Mestre_Final.csv JÁ EXISTE e não está vazio.")
            print("PULANDO FASES 4 & 5 (CONSOLIDAÇÃO).")
            print("=" * 100)
            return True
            
        print("\n" + "=" * 70)
        print("FASES 4/5: INICIANDO CONSOLIDAÇÃO NO CSV MESTRE ÚNICO")
        print(f"O CSV Mestre será gerado em: {os.path.abspath(caminho_saida_final)}")
        print("=" * 70)

        try:
            # Abre o arquivo de saída para escrita (modo 'w' para criar/sobrescrever)
            with open(caminho_saida_final, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile, delimiter=DELIMITADOR_PADRAO, quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(CABECALHO_FINAL) # Escreve o cabeçalho
                
                mapa_final_index = {nome: idx for idx, nome in enumerate(CABECALHO_FINAL)}
                
                # Lista de todos os arquivos CSV/TXT em Temp_brutos e subpastas
                todos_arquivos_brutos = []
                for root, _, files in os.walk(self.diretorio_saida_trabalho):
                    for f in files:
                        # Extensões reais detectadas nos seus arquivos (ex: .ESTABELE, .EMPRECSV)
                        extensoes_reais = ('.csv', '.txt', 'estable', 'empree', 'sociocsv', 'natjucsv', 'paiscsv', 'moticsv', 'cnaecsv', 'qualscsv', '.simple')
                        
                        # Verifica se o final do nome do arquivo corresponde a uma das extensões
                        if f.lower().endswith(extensoes_reais): 
                            todos_arquivos_brutos.append((os.path.join(root, f), f))
                
                if not todos_arquivos_brutos:
                    # Se não há arquivos brutos, mas o processo deve seguir
                    print("\nAVISO: NENHUM ARQUIVO CSV/TXT/BRUTO FOI ENCONTRADO na pasta Temp_brutos. O CSV Mestre ficará apenas com o cabeçalho.")
                    return True 


                # Iteração principal sobre CADA arquivo bruto encontrado
                for caminho_completo, nome_arquivo in tqdm(
                    todos_arquivos_brutos,
                    desc="Progresso Consolidação",
                    unit="arquivo"
                ):
                    
                    # 1. Tenta encontrar a CATEGORIA (EMPRE, ESTABELE, SOCIO, etc.)
                    nome_arquivo_maiusculo = nome_arquivo.upper()
                    nome_pasta_pai = os.path.basename(os.path.dirname(caminho_completo)).upper()
                    chave_busca = nome_pasta_pai + " " + nome_arquivo_maiusculo 
                    
                    nome_tipo_encontrado = None
                    
                    # Lógica de mapeamento flexível (baseada no nome da pasta/arquivo)
                    if 'EMPRESA' in chave_busca: nome_tipo_encontrado = 'EMPRE'
                    elif 'ESTABELECIMENTO' in chave_busca: nome_tipo_encontrado = 'ESTABELE'
                    elif 'SOCIO' in chave_busca: nome_tipo_encontrado = 'SOCIO'
                    elif 'CNAES' in chave_busca: nome_tipo_encontrado = 'CNAES'
                    elif 'MOTIVO' in chave_busca: nome_tipo_encontrado = 'MOTIVOS'
                    elif 'MUNIC' in chave_busca: nome_tipo_encontrado = 'MUNIC'
                    elif 'NATJU' in chave_busca: nome_tipo_encontrado = 'NATJU'
                    elif 'PAIS' in chave_busca: nome_tipo_encontrado = 'PAIS'
                    elif 'QUALI' in chave_busca: nome_tipo_encontrado = 'QUALS'
                    elif 'SIMPLES' in chave_busca: nome_tipo_encontrado = 'SIMPLES'

                    
                    if not nome_tipo_encontrado:
                        continue 

                    mapa_posicional = MAPA_COLUNAS_CONSOLIDADO[nome_tipo_encontrado]


                    try:
                        delimitador_real = self._detectar_delimitador(caminho_completo)
                        
                        # errors='ignore' para evitar que o Python trave em caracteres estranhos
                        with open(caminho_completo, 'r', encoding=ENCODING_LEITURA, errors='ignore') as infile:
                            reader = csv.reader(infile, delimiter=delimitador_real, quotechar='"')
                            
                            for linha_bruta in reader:
                                linha_mestre = [''] * len(CABECALHO_FINAL)
                                
                                # Mapeamento e Transferência de dados
                                for idx_bruto, nome_final in mapa_posicional:
                                    if idx_bruto < len(linha_bruta):
                                        valor = linha_bruta[idx_bruto].strip()
                                        
                                        if nome_final in mapa_final_index:
                                            idx_final = mapa_final_index[nome_final]
                                            linha_mestre[idx_final] = valor
                                            
                                linha_mestre[-1] = nome_tipo_encontrado # Adiciona a coluna de origem
                                writer.writerow(linha_mestre)

                    except Exception as e:
                        print(f"\n  !!! ERRO ao processar o arquivo {nome_arquivo} (Tipo: {nome_tipo_encontrado}) com delimitador '{delimitador_real or 'Padrão'}' [Pulando]: {e}")
                        
        except Exception as e:
            print(f"\n🛑 ERRO FATAL ao escrever o arquivo mestre ou na estrutura principal: {e}")
            return False 
            
        print(f"\n✅ CONSOLIDAÇÃO CONCLUÍDA! O CSV MESTRE ÚNICO foi gerado com sucesso.")
        return True
    
    # ==========================================================================
    # FASE 6: LIMPEZA (COMENTADA NESTE ARQUIVO, AGORA USAMOS O cleaner_cnpj.py)
    # ==========================================================================
    # A função fase_6_limpar_arquivos foi removida daqui, pois o run_pipeline.py
    # chama o cleaner_cnpj.py como FASE 6.
    pass # Mantemos a classe para que o wrapper funcione


# ==============================================================================
# FUNÇÃO WRAPPER PARA O ORQUESTRADOR
# ==============================================================================

def executar_consolidacao():
    """
    Função principal wrapper para o Orquestrador Mestre (Fases 4/5).
    Retorna True em caso de sucesso ou False em caso de falha.
    """
    try:
        # A verificação de 'tqdm' agora é tratada pelo bloco de importação no run_pipeline.py
        
        processador = ProcessadorConsolidacaoELimpeza()

        if not processador.diretorio_periodo:
              print("ERRO: Não foi possível encontrar a pasta de dados mais recente (AAAA-MM) em Dados_CNPJ.")
              return False

        if not os.path.exists(processador.diretorio_saida_trabalho):
            # Esta verificação é CRÍTICA, pois garante que as Fases 2/3 ocorreram.
            print("ERRO: Pasta de trabalho 'Temp_brutos' não encontrada. Verifique se as Fases 2/3 (Descompactação) falharam. Executando 'pip install pandas tqdm' pode resolver.")
            return False
            
        if not processador.fase_4_5_consolidar_csv_mestre():
            print("\nFALHA CRÍTICA: O processo de consolidação falhou.")
            return False
            
        print("\n" + "=" * 100)
        print("FASE 4/5 (CONSOLIDAÇÃO) CONCLUÍDA COM SUCESSO.")
        print("Os dados estão prontos no CSV MESTRE FINAL.")
        print("=" * 100)
        return True

    except FileNotFoundError as e:
        print(f"\n--- ERRO FATAL ---\n{e}")
        return False
    except Exception as e:
        print(f"\n--- ERRO INESPERADO ---\nOcorreu um erro inesperado na fase de Consolidação: {e}")
        return False


if __name__ == '__main__':
    executar_consolidacao()