# Este é o Módulo Core (Motor de Filtragem e Enriquecimento) da sua Plataforma de Leads.
# Ele usa a biblioteca Pandas (a Betoneira) para processar os dados.

# 1. Puxando a Betoneira (Pandas)
import pandas as pd
import random 
import time

# =========================================================================
# CONFIGURAÇÃO DE EXIBIÇÃO DO PANDAS (FORÇAR TODAS AS COLUNAS NO TERMINAL)
# =========================================================================
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None) 


# -------------------------------------------------------------------------
# FUNÇÕES DE SIMULAÇÃO DE CHAMADA DE API (MÓDULO III - Enriquecimento Completo)
# -------------------------------------------------------------------------
def simular_busca_google(razao_social, cidade):
    """
    Simula a chamada a uma API do Google Search para encontrar o SITE.
    """
    time.sleep(0.01) # Simula latência
    
    if ('Soft' in razao_social or 'Mark' in razao_social) and 'São Paulo' in cidade:
        if 'Software Solutions' in razao_social:
             return "https://www.softwaresolutions.com.br"
        elif 'Marketing Digital' in razao_social:
             return "https://www.marketingdigitalpro.com"
        return f"https://www.{razao_social.lower().replace(' ', '').replace('ltda', '').replace('pro', '')}.com.br"
        
    return "N/A"

def simular_busca_email(site):
    """
    Simula a busca do e-mail de contato no site enriquecido.
    """
    time.sleep(0.01) # Simula latência
    if site != "N/A" and "softwaresolutions" in site:
        return "contato@softwaresolutions.com.br"
    elif site != "N/A" and "marketingdigitalpro" in site:
        return "vendas@marketingdigitalpro.com"
    return "N/A"

def simular_busca_linkedin(razao_social):
    """
    Simula a busca pelo perfil da empresa no LinkedIn.
    """
    time.sleep(0.01) # Simula latência
    if 'Software Solutions' in razao_social:
        return "https://linkedin.com/company/softwares-solutions"
    elif 'Serviços AWS Brasil' in razao_social:
        return "https://linkedin.com/company/aws-br-oficial"
    return "N/A"

def simular_busca_contato_digital(razao_social):
    """
    Simula a busca por Celular, WhatsApp, Instagram e Facebook.
    Retorna um dicionário com os resultados.
    """
    time.sleep(0.01)
    if 'Software Solutions' in razao_social:
        return {
            'WHATSAPP': '(11) 98765-4321', 
            'INSTAGRAM': '@softwaresolutionsbr', 
            'FACEBOOK': 'facebook.com/softwaresolutions',
            'CELULAR_DONO': '(11) 99999-0000'
        }
    elif 'Marketing Digital' in razao_social:
        return {
            'WHATSAPP': '(11) 91234-5678', 
            'INSTAGRAM': '@marketingpro', 
            'FACEBOOK': 'N/A',
            'CELULAR_DONO': 'N/A'
        }
    return {'WHATSAPP': 'N/A', 'INSTAGRAM': 'N/A', 'FACEBOOK': 'N/A', 'CELULAR_DONO': 'N/A'}

def simular_busca_google_maps(razao_social):
    """
    Simula a busca por perfil ativo no Google Maps.
    """
    time.sleep(0.01)
    if 'Software Solutions' in razao_social or 'Marketing Digital' in razao_social:
        return "Sim (Otimizado)"
    return "Não"


# -------------------------------------------------------------------------
# 2. Criando a Tabela de CNPJs (Simulação COMPLETA)
# -------------------------------------------------------------------------
dados_simulados = {
    'CNPJ': [f'123456780001{i:02d}' for i in range(1, 15)],
    'RAZAO_SOCIAL': ['Software Solutions Ltda', 'Consultoria XP', 'Imobiliária Central', 'Clínica Sorriso', 'Marketing Digital Pro', 
                    'Academia Corpo Livre', 'Tech Startup 10', 'Restaurante Sabor', 'Serviços AWS Brasil', 'Desenvolvimento Ágil',
                    'Serviços Contábeis SP', 'E-commerce Moda', 'Startup de Pagamentos', 'Agência de Viagens'],
    'CNAE': ['6201600', '7020400', '6810200', '8630500', '7319000', 
            '9313100', '6201600', '5611201', '6201600', '6201600',
            '6920601', '4781400', '6201600', '7911200'],
    'CIDADE': ['São Paulo', 'Rio de Janeiro', 'São Paulo', 'Belo Horizonte', 'São Paulo', 
                'Curitiba', 'Rio de Janeiro', 'Belo Horizonte', 'São Paulo', 'Curitiba', 
                'São Paulo', 'Rio de Janeiro', 'São Paulo', 'Belo Horizonte'],
    'UF': ['SP', 'RJ', 'SP', 'MG', 'SP', 'PR', 'RJ', 'MG', 'SP', 'PR', 'SP', 'RJ', 'SP', 'MG'],
    
    # NOVOS CAMPOS DE ENDEREÇO DETALHADO
    'RUA': [
        'Av. Paulista', 'R. Sete de Setembro', 'Av. Faria Lima', 'R. da Bahia',
        'R. Augusta', 'R. XV de Novembro', 'Praia de Botafogo', 'Av. Afonso Pena',
        'Av. Eng. Luis Carlos Berrini', 'R. Ébano Pereira', 'R. Consolação',
        'Av. Atlântica', 'Av. Brigadeiro Faria Lima', 'R. Rio de Janeiro'
    ],
    'BAIRRO': [
        'Bela Vista', 'Centro', 'Itaim Bibi', 'Lourdes', 'Consolação',
        'Centro', 'Botafogo', 'Centro', 'Brooklin', 'Centro',
        'Consolação', 'Copacabana', 'Itaim Bibi', 'Lourdes'
    ],
    'NUMERO_ESPECIFICACAO': [
        '1000, 10º Andar', '50, Sala 201', '3000', '1200', '800, Loja A', 
        '150', '400', '2500, Térreo', '1700, Torre A', '30', '1900, Fundos', 
        '500', '4000', '100'
    ],
    
    # CAMPOS ORIGINAIS
    'ENDERECO_COMPLETO': [
        'Av. Paulista, 1000', 'R. Sete de Setembro, 50', 'Av. Faria Lima, 3000', 'R. da Bahia, 1200',
        'R. Augusta, 800', 'R. XV de Novembro, 150', 'Praia de Botafogo, 400', 'Av. Afonso Pena, 2500',
        'Av. Eng. Luis Carlos Berrini, 1700', 'R. Ébano Pereira, 30', 'R. Consolação, 1900',
        'Av. Atlântica, 500', 'Av. Brigadeiro Faria Lima, 4000', 'R. Rio de Janeiro, 100'
    ],
    'NOME_SOCIO_ADMINISTRADOR': [
        'João Silva', 'Maria Souza', 'Carlos Oliveira', 'Ana Paula Santos', 
        'Pedro Costa', 'Fernanda Lima', 'Ricardo Mendes', 'Camila Pires',
        'Roberto Dias', 'Luciana Gomes', 'Antônio Ferreira', 'Juliana Nunes',
        'Daniel Barbosa', 'Laura Martins'
    ], 
    'PORTE': ['LTDA', 'ME', 'EPP', 'MEI', 'ME', 'LTDA', 'EPP', 'MEI', 'LTDA', 'ME',
              'LTDA', 'EPP', 'ME', 'LTDA'],
    'CAPITAL_SOCIAL': [500000, 10000, 80000, 5000, 20000, 
                       100000, 150000, 500, 450000, 30000,
                       25000, 50000, 75000, 120000],

    # NOVOS CAMPOS DE CONTATO E ENRIQUECIMENTO (VAZIOS)
    'CPF_DONO': ['MOCKADO_123'] * 14, # NOTA: CPF é um dado sensível, aqui é apenas um MOCK. Não é buscado via API pública.
    'CELULAR_DONO': ['N/A'] * 14,
    'WHATSAPP': ['N/A'] * 14,
    'INSTAGRAM': ['N/A'] * 14,
    'FACEBOOK': ['N/A'] * 14,
    'SITE': ['N/A'] * 14,
    'EMAIL_CONTATO': ['N/A'] * 14,
    'LINKEDIN': ['N/A'] * 14,
    'GOOGLE_MAPS_PERFIL': ['N/A'] * 14,
}

# Criamos o DataFrame (a tabela do Pandas)
df = pd.DataFrame(dados_simulados)

# =========================================================================
# 3. MÓDULO II: DEFINIÇÃO DE FILTROS MÚLTIPLOS (Otimização)
# =========================================================================

# Filtros que queremos aplicar (múltiplos valores são permitidos)
cnae_alvo = ['6201600', '7319000'] 
porte_alvo = ['EPP', 'ME', 'LTDA']
cidade_alvo = ['São Paulo', 'Curitiba']


# =========================================================================
# 4. COMANDO MÁGICO DE FILTRAGEM (Módulo I e II)
# =========================================================================
filtro_combinado = (
    (df['CNAE'].isin(cnae_alvo)) & 
    (df['PORTE'].isin(porte_alvo)) &
    (df['CIDADE'].isin(cidade_alvo))
)

leads_filtrados = df[filtro_combinado].copy()

# =========================================================================
# 5. MÓDULO II: ATRIBUIÇÃO DE SCORE DE POTENCIAL
# =========================================================================
def calcular_score(row):
    score = 0
    
    if row['PORTE'] == 'LTDA':
        score += 3
    elif row['PORTE'] == 'EPP':
        score += 2
    elif row['PORTE'] == 'ME':
        score += 1
        
    if row['CAPITAL_SOCIAL'] >= 100000:
        score += 3
    elif row['CAPITAL_SOCIAL'] >= 50000:
        score += 2
    else:
        score += 1
        
    final_score = min(score, 5)
    return final_score

leads_filtrados['SCORE'] = leads_filtrados.apply(calcular_score, axis=1)

def classificar_potencial(score):
    if score >= 4:
        return 'ALTO'
    elif score >= 3:
        return 'MÉDIO'
    else:
        return 'BAIXO'

leads_filtrados['POTENCIAL'] = leads_filtrados['SCORE'].apply(classificar_potencial)

leads_filtrados = leads_filtrados.sort_values(by=['SCORE', 'CAPITAL_SOCIAL'], ascending=False)


# =========================================================================
# 6. MÓDULO III: ENRIQUECIMENTO DE DADOS (Busca Completa)
# =========================================================================

print("\n----------------------------------------------")
print("🤖 MÓDULO III: Iniciando Enriquecimento COMPLETO (Site, Email, Redes, Maps)...")

# ETAPA 1: Busca do SITE (Pré-requisito para Email)
leads_filtrados['SITE'] = leads_filtrados.apply(
    lambda row: simular_busca_google(row['RAZAO_SOCIAL'], row['CIDADE']), 
    axis=1
)

# ETAPA 2: Busca do EMAIL (Depende do SITE)
leads_filtrados['EMAIL_CONTATO'] = leads_filtrados['SITE'].apply(simular_busca_email)

# ETAPA 3: Busca de Contatos e Redes Sociais (WhatsApp, Instagram, Facebook, Celular Dono)
contato_digital = leads_filtrados['RAZAO_SOCIAL'].apply(simular_busca_contato_digital).apply(pd.Series)

# Atualiza o DataFrame com os resultados do enriquecimento digital
for col in ['WHATSAPP', 'INSTAGRAM', 'FACEBOOK', 'CELULAR_DONO']:
    leads_filtrados[col] = contato_digital[col]

# ETAPA 4: Busca do LINKEDIN (Presença Profissional)
leads_filtrados['LINKEDIN'] = leads_filtrados['RAZAO_SOCIAL'].apply(simular_busca_linkedin)

# ETAPA 5: Busca do Perfil Google Maps
leads_filtrados['GOOGLE_MAPS_PERFIL'] = leads_filtrados['RAZAO_SOCIAL'].apply(simular_busca_google_maps)


print("✅ Enriquecimento concluído. (Simulação de APIs de Contato, Social e Maps)")
print("----------------------------------------------")

# =========================================================================
# 7. Exibindo o Resultado Final (Relatório Completo de Vendas)
# =========================================================================

# Colunas na ordem de importância para Prospecção B2B
colunas_exibicao = [
    # 1. IDENTIFICAÇÃO E POTENCIAL
    'POTENCIAL',
    'SCORE',
    'RAZAO_SOCIAL', 
    'CNAE',
    
    # 2. CONTATO IMEDIATO
    'EMAIL_CONTATO', 
    'CELULAR_DONO', 
    'WHATSAPP', 
    'LINKEDIN',
    'SITE',
    
    # 3. PRESENÇA E ENDEREÇO DETALHADO
    'INSTAGRAM', 
    'FACEBOOK', 
    'GOOGLE_MAPS_PERFIL', 
    'CIDADE', 
    'RUA', 
    'BAIRRO', 
    'NUMERO_ESPECIFICACAO', 
    'NOME_SOCIO_ADMINISTRADOR',
    'CPF_DONO',
]

leads_final = leads_filtrados[colunas_exibicao]

# Contagem de sucesso de enriquecimento
leads_enriquecidos = len(leads_filtrados[
    (leads_filtrados['SITE'] != 'N/A') | 
    (leads_filtrados['EMAIL_CONTATO'] != 'N/A') | 
    (leads_filtrados['LINKEDIN'] != 'N/A') |
    (leads_filtrados['WHATSAPP'] != 'N/A') |
    (leads_filtrados['GOOGLE_MAPS_PERFIL'] != 'Não')
])


print("==============================================")
print(f"✅ Leads Qualificados e Enriquecidos:")
print(f"🔍 Total de Leads com pelo menos 1 enriquecimento de contato/presença: {leads_enriquecidos}")
print("----------------------------------------------")
print("💰 TABELA FINAL DE INTELIGÊNCIA B2B (RELATÓRIO COMPLETO):")
# Exibe a tabela completa (filtrada e enriquecida)
print(leads_final)
print("==============================================")
