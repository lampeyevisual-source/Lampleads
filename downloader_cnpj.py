import requests
from bs4 import BeautifulSoup
import os
import re
import time 
from tqdm import tqdm 

# --- Configurações ---
DIRETORIO_BASE = 'Dados_CNPJ' 
URL_BASE = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/'
MAX_RETRIES = 5 
RETRY_DELAY = 10 

# --- Funções Auxiliares ---

def encontrar_diretorio_mais_recente(url):
    """Busca a página e retorna o nome do subdiretório mais recente (ex: '2025-11')."""
    print(f"Buscando o diretório mais recente em: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        padrao_data = re.compile(r'^\d{4}-\d{2}/$')
        
        diretorios_encontrados = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and padrao_data.match(href):
                diretorios_encontrados.append(href.strip('/'))

        if not diretorios_encontrados:
            print("ERRO: Nenhum diretório de data (AAAA-MM) foi encontrado.")
            return None
            
        diretorio_recente = sorted(diretorios_encontrados, reverse=True)[0]
        
        print(f"Diretório de período mais recente encontrado no site da RF: {diretorio_recente}")
        return diretorio_recente
        
    except requests.exceptions.RequestException as e:
        print(f"ERRO ao acessar a URL base: {e}")
        return None

def encontrar_arquivos_zip(url_diretorio):
    """Busca a página da versão e retorna uma lista de URLs de arquivos .zip."""
    print(f"Buscando arquivos .zip em: {url_diretorio}")
    try:
        response = requests.get(url_diretorio)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        arquivos_zip = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.lower().endswith('.zip') and not href.endswith('/'):
                arquivos_zip.append(url_diretorio + href)
        
        return arquivos_zip
        
    except requests.exceptions.RequestException as e:
        print(f"ERRO ao acessar a URL do diretório de dados: {e}")
        return []

def obter_arquivos_existentes(diretorio_destino):
    """Retorna um set com os nomes dos arquivos .zip que já existem no diretório de destino (e têm tamanho mínimo)."""
    if not os.path.exists(diretorio_destino):
        return set()
        
    arquivos_locais = set()
    for nome in os.listdir(diretorio_destino):
        if nome.lower().endswith('.zip') and os.path.isfile(os.path.join(diretorio_destino, nome)):
            # Garante que o arquivo não é um arquivo "vazio" (0KB).
            if os.path.getsize(os.path.join(diretorio_destino, nome)) > 1024:
                arquivos_locais.add(nome)
            
    return arquivos_locais

def baixar_arquivo(url_arquivo, diretorio_destino):
    """Baixa o arquivo .zip com retry e barra de progresso."""
    nome_arquivo = url_arquivo.split('/')[-1]
    caminho_completo = os.path.join(diretorio_destino, nome_arquivo)
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"-> Baixando {nome_arquivo} (Tentativa {attempt + 1}/{MAX_RETRIES})...")
            
            response = requests.get(url_arquivo, stream=True, timeout=300) 
            response.raise_for_status() 

            total_size_in_bytes = int(response.headers.get('content-length', 0))
            block_size = 8192 
            
            with open(caminho_completo, 'wb') as file:
                with tqdm(
                    desc=f"  {nome_arquivo}",
                    total=total_size_in_bytes,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
                    leave=False 
                ) as bar:
                    for chunk in response.iter_content(block_size):
                        bar.update(len(chunk))
                        file.write(chunk)

            print(f"    Download de {nome_arquivo} concluído com sucesso.")
            return True

        except (requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout, 
                requests.exceptions.RequestException,
                ConnectionResetError, 
                ConnectionAbortedError) as e:
            
            print(f"    ERRO ao baixar {nome_arquivo} (Tentativa {attempt + 1}): {e}")
            
            if attempt < MAX_RETRIES - 1:
                print(f"    Aguardando {RETRY_DELAY}s para tentar novamente...")
                time.sleep(RETRY_DELAY)
                
                # Tenta remover o arquivo parcial
                if os.path.exists(caminho_completo):
                    try:
                        os.remove(caminho_completo)
                        print(f"    Arquivo parcial {nome_arquivo} removido.")
                    except Exception:
                        pass 
            else:
                print(f"    Limite de {MAX_RETRIES} tentativas excedido para {nome_arquivo}. Falha final.")
                # Tenta remover o arquivo parcial na falha final
                if os.path.exists(caminho_completo):
                    try:
                        os.remove(caminho_completo)
                    except Exception:
                        pass
                return False

# --- Lógica Principal da Fase (executar_download) ---

def executar_download():
    """FASE 1: Encontra a versão mais recente e baixa todos os arquivos ZIPs."""
    
    # 0. Verifica se as dependências estão instaladas (para um erro limpo)
    try:
        import requests
        from tqdm import tqdm
    except ImportError:
        print("\n" + "=" * 70)
        print("ERRO: As bibliotecas 'requests' ou 'tqdm' não foram encontradas.")
        print("Execute o comando: pip install requests tqdm")
        print("=" * 70)
        return False 

    diretorio_versao = encontrar_diretorio_mais_recente(URL_BASE)
    
    if not diretorio_versao:
        print("\nProcesso encerrado devido a falha ao encontrar o diretório de período.")
        return False 

    diretorio_destino = os.path.join(DIRETORIO_BASE, diretorio_versao) 
    url_diretorio = URL_BASE + diretorio_versao + '/'
    
    arquivos_zip_urls = encontrar_arquivos_zip(url_diretorio)

    if not arquivos_zip_urls:
        print("\nNenhum arquivo ZIP foi encontrado para download. Processo encerrado.")
        # Se o site não tem arquivos, a fase pode prosseguir (embora não haja dados).
        return True 

    # 1. Cria o diretório de destino
    if not os.path.exists(diretorio_destino):
        print(f"\nCriando diretório para o período {diretorio_versao}: {diretorio_destino}")
        os.makedirs(diretorio_destino)
    else:
        print(f"\nDiretório para o período {diretorio_versao} já existe: {diretorio_destino}")

    # 2. Obtém lista de arquivos já baixados/existentes
    arquivos_locais = obter_arquivos_existentes(diretorio_destino)
    
    # 3. Determina quais arquivos precisam ser baixados
    arquivos_a_baixar_urls = []
    
    for url in arquivos_zip_urls:
        nome_arquivo = url.split('/')[-1]
        if nome_arquivo not in arquivos_locais:
            arquivos_a_baixar_urls.append(url)
        else:
            print(f"-> {nome_arquivo}: Encontrado localmente e completo. Pulando download.")
            
    # 4. Início do Download
    
    total_arquivos = len(arquivos_zip_urls)
    arquivos_restantes = len(arquivos_a_baixar_urls)
    
    print("-" * 45)
    print(f"Total de arquivos na RF: {total_arquivos}")
    print(f"Arquivos já existentes/concluídos: {total_arquivos - arquivos_restantes}")
    print(f"Arquivos pendentes/faltantes: {arquivos_restantes}")
    print("-" * 45)

    if not arquivos_a_baixar_urls:
        print("TODOS OS ARQUIVOS JÁ ESTÃO BAIXADOS. Processo encerrado.")
        return True 

    downloads_concluidos = total_arquivos - arquivos_restantes 
    downloads_com_falha = 0
    
    for url_arquivo in arquivos_a_baixar_urls:
        if baixar_arquivo(url_arquivo, diretorio_destino):
            downloads_concluidos += 1
        else:
            downloads_com_falha += 1

    print("-" * 45)
    print(f"Processo de download finalizado.")
    print(f"Total de arquivos concluídos/existentes: {downloads_concluidos}")
    print(f"Total de arquivos com falha (após retries): {downloads_com_falha}")
    print("-" * 45)
    
    # A FASE SÓ TERMINA COM SUCESSO SE TODOS OS ARQUIVOS ESTIVEREM LÁ.
    return downloads_concluidos == total_arquivos


if __name__ == '__main__':
    executar_download()