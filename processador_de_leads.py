# processador_de_leads.py - Módulo de Filtro, Agregação Total e Geração de HTML (Versão Otimizada)

import pandas as pd
import numpy as np
import os
import re
from tqdm import tqdm
from typing import List, Optional

# --- Configurações de Caminho e Agregação ---
DIRETORIO_BASE = 'Dados_CNPJ'
NOME_ARQUIVO_MESTRE = 'CSV_Mestre_Final.csv'
SEPARADOR_AGREGACAO = ' | ' # Separador para juntar múltiplos valores (ex: Sócios, CNAEs)

# ==============================================================================
# FUNÇÕES DE UTILIDADE (Com correção para encontrar o caminho)
# ==============================================================================

def _encontrar_caminho_mestre() -> Optional[str]:
    """Localiza o caminho completo para o CSV Mestre mais recente (Versão Corrigida)."""
    try:
        # 1. Encontra a subpasta de período (AAAA-MM) mais recente
        itens = os.listdir(DIRETORIO_BASE)
        padrao_data = re.compile(r'^\d{4}-\d{2}$')
        diretorios_de_periodo = [
            item for item in itens 
            if os.path.isdir(os.path.join(DIRETORIO_BASE, item)) and padrao_data.match(item)
        ]
        
        if not diretorios_de_periodo:
            print("AVISO: Nenhuma pasta AAAA-MM encontrada dentro de Dados_CNPJ.") 
            return None

        # Pega a pasta mais recente (Ex: 2025-11)
        diretorio_recente_nome = sorted(diretorios_de_periodo, reverse=True)[0]
        diretorio_periodo = os.path.join(DIRETORIO_BASE, diretorio_recente_nome)
        
        caminho_mestre = os.path.join(diretorio_periodo, NOME_ARQUIVO_MESTRE)
        
        if not os.path.exists(caminho_mestre):
            print(f"AVISO: Arquivo CSV Mestre não encontrado em: {caminho_mestre}.")
            return None
        
        return caminho_mestre

    except FileNotFoundError:
        print(f"ERRO CRÍTICO: O diretório base '{DIRETORIO_BASE}' não foi encontrado. Crie a pasta e execute o pipeline de ETL.")
        return None
    except Exception as e:
        print(f"ERRO inesperado ao buscar caminho mestre: {e}")
        return None

# ==============================================================================
# 1. FUNÇÃO PRINCIPAL: FILTRAGEM E PRÉ-PROCESSAMENTO
# ==============================================================================

def aplicar_inteligencia_e_filtrar_leads(caminho_mestre: str, arquivo_html: str) -> bool:
    """
    Lê o CSV Mestre (com otimização de memória), aplica agregação total, filtra e gera o HTML.
    """
    print("=" * 80)
    print("FASE 7: INICIANDO PROCESSAMENTO DE LEADS (AGREGAÇÃO DE DADOS COMPLETOS)")
    print(f"Lendo dados de: {caminho_mestre}")
    print("=" * 80)

    # 1. LEITURA DOS DADOS (COM OTIMIZAÇÃO DE MEMÓRIA CRÍTICA)
    try:
        # Mapeamento de tipos para economizar memória (Reduz o uso de RAM de 11GB para 3-5GB)
        dtype_spec = {
            # Tipos Categóricos/Códigos (Repetição de valores)
            'situacao_cadastral': 'category',
            'porte_empresa': 'category',
            'codigo_municipio': 'category',
            'uf': 'category',
            'matriz_filial': 'category',
            'TABELA_ORIGEM': 'category',
            'cnae_fiscal_principal': 'category',
            
            # Manter como 'object' para strings longas ou variáveis
            'cnae_fiscal_secundario': 'object',
            'nome_socio': 'object', 
            
            # Tipos Numéricos (CNPJs e Datas, tratados como strings para manter zeros à esquerda)
            'cnpj_basico': 'string',
            'cnpj_ordem': 'string',
            'cnpj_dv': 'string',
            'data_inicio_atividade': 'string',
            'data_situacao_cadastral': 'string',
            
            # Capital Social e strings longas (Nomes e Endereços)
            'capital_social': np.float64, # Usar float para o capital
            'razao_social': 'string',
            'nome_fantasia': 'string',
            'correio_eletronico': 'string',
        }
        
        df = pd.read_csv(
            caminho_mestre, 
            sep=';', 
            encoding='utf-8', 
            dtype=dtype_spec, # Usa a especificação de tipo para otimizar
            low_memory=False, # Requer False para dtype_spec funcionar bem
            keep_default_na=False
        )
        # Substitui strings vazias por NaN para agregação
        df = df.replace('', np.nan) 

    except Exception as e:
        print(f"🛑 ERRO: Falha ao carregar o CSV Mestre. {e}")
        print("Pode ser um erro de memória. Tente fechar outros programas e reexecutar.")
        return False
    
    print(f"Dados carregados. Linhas totais: {len(df)}")


    # 2. AGREGAÇÃO E CONCATENAÇÃO DE DADOS MÚLTIPLOS (NÃO PERDER INFORMAÇÕES)
    
    COLUNAS_MANTER_PRIMEIRO = [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 
        'razao_social', 'nome_fantasia', 'data_inicio_atividade', 
        'situacao_cadastral', 'data_situacao_cadastral', 'capital_social',
        'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'nome_municipio',
        'ddd_1', 'telefone_1', 'correio_eletronico', 'cnae_fiscal_principal', 'porte_empresa'
    ]
    
    COLUNAS_AGREGAR = [
        'nome_socio', 'cpf_cnpj_socio', 'qualificacao_socio', 
        'cnae_fiscal_secundario'
    ]

    # Função para agregar valores
    def aggregate_data(series):
        unique_values = series.dropna().unique()
        return SEPARADOR_AGREGACAO.join(unique_values) if unique_values.size > 0 else np.nan

    # Executa a agregação nas colunas específicas
    df_agregado = df.groupby('cnpj_basico')[COLUNAS_AGREGAR].agg(aggregate_data).reset_index()

    # Mantém a primeira ocorrência das colunas únicas (e mais importantes)
    df_manter = df.drop_duplicates(subset='cnpj_basico', keep='first')[COLUNAS_MANTER_PRIMEIRO]
    
    # Junta as duas partes para formar o DataFrame final de leads
    df_leads = pd.merge(df_manter, df_agregado, on='cnpj_basico', how='left')
    
    print(f"Linhas consolidadas e agregadas (CNPJ Básico Único): {len(df_leads)}")


    # 3. FILTROS DE INTELIGÊNCIA CRÍTICA (Garantindo Leads de Qualidade)
    
    # 3.1. CNPJ Ativo
    df_leads = df_leads[df_leads['situacao_cadastral'] == '1']
    print(f"- Filtro Ativo (situacao_cadastral=1): {len(df_leads)}")


    # 4. GERAÇÃO DA ESTRUTURA FINAL
    COLUNAS_SITE_AGREGADAS = COLUNAS_MANTER_PRIMEIRO + COLUNAS_AGREGAR 
    df_final = df_leads[COLUNAS_SITE_AGREGADAS].copy()
    
    print(f"Dados prontos para injeção HTML: {len(df_final)}")
    
    # 5. GERAR HTML
    html_gerado = gerar_conteudo_html(df_final, SEPARADOR_AGREGACAO)
    
    # 6. INJETAR NO TEMPLATE
    injetar_html_no_template(html_gerado, arquivo_html)

    return True


# ==============================================================================
# 2. FUNÇÃO: GERAÇÃO DE CONTEÚDO HTML (Cria a estrutura legível)
# ==============================================================================

def gerar_conteudo_html(df_final: pd.DataFrame, separador: str) -> str:
    """
    Transforma cada linha do DataFrame em um bloco HTML formatado (Card de Lead).
    """
    html_blocos: List[str] = []
    
    for _, row in tqdm(df_final.iterrows(), total=len(df_final), desc="Gerando HTML dos Leads"):
        
        # --- Formatação de Dados para Legibilidade ---
        cnpj_completo = f"{row['cnpj_basico']}{row['cnpj_ordem']}{row['cnpj_dv']}"
        cnpj_formatado = f"{cnpj_completo[:8]}.{cnpj_completo[8:12]}-{cnpj_completo[12:]}"
        
        # O Pandas, usando float64, pode representar o capital social como um número.
        capital_float = float(row['capital_social']) if pd.notna(row['capital_social']) else 0.0
        # Formato monetário com milhares: R$ 1.234.567,89
        capital_social = f"R$ {capital_float:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
        
        # --- Processamento das Colunas Agregadas (SÓCIOS) ---
        lista_socios = row['nome_socio'].split(separador) if pd.notna(row['nome_socio']) and isinstance(row['nome_socio'], str) else ['Nenhum Sócio Encontrado']
        socios_html = "".join([f"<li>{nome_socio.strip()}</li>" for nome_socio in lista_socios])

        # Processamento dos CNAES Secundários
        cnaes_secundarios = row['cnae_fiscal_secundario'].split(separador) if pd.notna(row['cnae_fiscal_secundario']) and isinstance(row['cnae_fiscal_secundario'], str) else ['Nenhum']
        cnaes_sec_html = ", ".join([cnae.strip() for cnae in cnaes_secundarios])
        
        # --- Monta o Bloco HTML ---
        card_html = f"""
        <div class="lead-card">
            <h3 class="razao-social">**{row['razao_social'] or 'N/A'}** ({row['nome_fantasia'] or 'N/A'})</h3>
            <p class="cnpj-info">CNPJ: {cnpj_formatado} | Porte: {row['porte_empresa'] or 'N/A'}</p>
            
            <div class="secao-societaria">
                <h4>Estrutura Societária Completa (Agregada):</h4>
                <ul class="lista-socios">
                    {socios_html}
                </ul>
            </div>
            
            <div class="detalhes-financeiros">
                <p><strong>Capital Social:</strong> {capital_social}</p>
                <p><strong>Status Legal:</strong> ATIVA desde {row['data_inicio_atividade'] or 'N/A'}</p>
            </div>
            
            <div class="contato-e-localizacao">
                <p>📍 {row['logradouro'] or 'S/N'}, {row['numero'] or 'N/A'} - {row['bairro'] or 'N/A'}, {row['nome_municipio'] or 'N/A'}/{row['uf'] or 'N/A'}</p>
                <p>📞 ({row['ddd_1'] or '00'}) {row['telefone_1'] or 'N/A'} | 📧 {row['correio_eletronico'] or 'N/A'}</p>
            </div>
            
            <div class="cnaes">
                <p><strong>CNAE Principal:</strong> {row['cnae_fiscal_principal'] or 'N/A'}</p>
                <p><strong>CNAEs Secundários:</strong> {cnaes_sec_html}</p>
            </div>
            
            <hr>
        </div>
        """
        html_blocos.append(card_html)
        
    return "\n".join(html_blocos)

# ==============================================================================
# 3. FUNÇÃO: INJEÇÃO NO TEMPLATE HTML
# ==============================================================================

def injetar_html_no_template(html_conteudo: str, caminho_template: str) -> bool:
    """
    Injeta o conteúdo HTML gerado no arquivo HTML de destino, usando um placeholder.
    """
    PLACEHOLDER_TAG = "<!-- LEADS_CONTENT_HERE -->" # Certificar que o placeholder está correto
    
    try:
        with open(caminho_template, 'r', encoding='utf-8') as f:
            template_html = f.read()
            
        if PLACEHOLDER_TAG not in template_html:
            print(f"AVISO: O placeholder '{PLACEHOLDER_TAG}' não foi encontrado no {caminho_template}. O conteúdo não será injetado.")
            return False
            
        html_final = template_html.replace(PLACEHOLDER_TAG, html_conteudo)
        
        with open(caminho_template, 'w', encoding='utf-8') as f:
            f.write(html_final)
            
        print(f"✅ CONTEÚDO INJETADO! O arquivo {caminho_template} foi atualizado com sucesso.")
        return True
        
    except FileNotFoundError:
        print(f"🛑 ERRO: O arquivo template {caminho_template} não foi encontrado.")
        return False
    except Exception as e:
        print(f"🛑 ERRO ao injetar conteúdo no HTML: {e}")
        return False


# ==============================================================================
# WRAPPER PRINCIPAL
# ==============================================================================

def executar_processamento_leads(nome_arquivo_html: str = 'index.html') -> bool:
    """Orquestra as fases de leitura, filtragem e geração de HTML."""
    caminho_mestre = _encontrar_caminho_mestre()
    
    if not caminho_mestre:
        print("FALHA: Não foi possível localizar o CSV Mestre Final. Verifique a pasta 'Dados_CNPJ' e re-execute o pipeline de ETL.")
        return False
    
    if aplicar_inteligencia_e_filtrar_leads(caminho_mestre, nome_arquivo_html):
        print("\n" + "=" * 100)
        print("FASE 7 (PROCESSAMENTO DE LEADS) CONCLUÍDA COM SUCESSO.")
        print(f"Seu dashboard/site {nome_arquivo_html} foi gerado/atualizado. Abra o arquivo no navegador.")
        print("=" * 100)
        return True
    
    return False

if __name__ == '__main__':
    executar_processamento_leads()