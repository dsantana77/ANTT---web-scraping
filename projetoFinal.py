import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# Diretório onde os arquivos serão salvos
download_dir = "projeto_1"
os.makedirs(download_dir, exist_ok=True)

# Parte Download

# URL da página que contém os dados
url = "https://dados.antt.gov.br/dataset/gerenciamento-de-autorizacoes"

# Função para fazer o download de um arquivo
def download_file(file_url, download_path):
    try:
        response = requests.get(file_url)
        response.raise_for_status()  # Lança uma exceção para códigos de status HTTP 4xx/5xx
        with open(download_path, 'wb') as file:
            file.write(response.content)
        print(f"Download concluído: {download_path}")
    except requests.exceptions.RequestException as e:
        print(f"Falha no download: {file_url} ({e})")

# Acessando a página principal
try:
    response = requests.get(url)
    response.raise_for_status()  # Lança uma exceção para códigos de status HTTP 4xx/5xx
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Encontrar todas as tags <a> que possivelmente contenham links para arquivos CSV
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('.csv'):
            file_name = href.split('/')[-1]
            
            # Verifica se o arquivo contém "horarios_" ou "linhas_secoes" e não é "historico_linhas_secoes"
            if ("horarios_" in file_name or "linhas_secoes" in file_name) and "historico_linhas_secoes" not in file_name:
                
                download_path = os.path.join(download_dir, file_name)
                
                # Verifica se o arquivo já existe antes de baixá-lo
                if not os.path.exists(download_path):
                    download_file(href, download_path)
                else:
                    print(f"Arquivo já existe: {file_name}")
            else:
                print(f"Arquivo ignorado: {file_name} (não corresponde aos critérios ou é histórico)")
except requests.exceptions.RequestException as e:
    print(f"Falha ao acessar {url} ({e})")
print("Processo de download foi finalizado")

# Parte Processamento

# Função para extrair a data do nome do arquivo
def extract_date_from_filename(file_name):
    try:
        date_str = file_name.split('_')[-2:]  # Pega os dois últimos elementos (mes e ano)
        date_str = '_'.join(date_str).replace('.csv', '')  # Junta e remove a extensão
        return datetime.strptime(date_str, "%m_%Y")
    except ValueError:
        return datetime.min

# Função para extrair a data de competência do nome do arquivo
def extract_competencia_from_filename(file_name):
    try:
        date_str = file_name.split('_')[-2:]  # Pega os dois últimos elementos (mes e ano)
        date_str = '_'.join(date_str).replace('.csv', '')  # Junta e remove a extensão
        competencia_date = datetime.strptime(date_str, "%m_%Y")
        return competencia_date.strftime("%Y-%m")  # Apenas ano e mês
    except ValueError:
        return None

# Função para processar arquivos
def process_files(file_filter, output_prefix):
    # Listar todos os arquivos na pasta
    files = os.listdir(download_dir)
    
    # Filtrar arquivos com base no filtro fornecido
    filtered_files = [f for f in files if file_filter in f and f.endswith('.csv')]

    # Lista para armazenar DataFrames
    dfs = []

    # Ordenar arquivos por data extraída do nome do arquivo
    filtered_files.sort(key=lambda f: extract_date_from_filename(f) or datetime.min)

    # Iterar sobre cada arquivo e carregar no DataFrame
    for file_name in filtered_files:
        file_path = os.path.join(download_dir, file_name)
        processed = False

        # Tentar carregar o CSV em um DataFrame com diferentes codificações e delimitadores
        for encoding in ['cp1252', 'utf-8', 'ISO-8859-1', 'latin1']:
            for delimiter in [';']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter)
                    
                    # Adicionar colunas
                    df['data_download'] = datetime.now().strftime("%Y-%m-%d")
                    df['data_competencia'] = extract_competencia_from_filename(file_name)
                    df['fonte'] = file_name
                    
                    # Aplicar trim nas colunas de texto
                    text_columns = df.select_dtypes(include=['object']).columns
                    for col in text_columns:
                        df[col] = df[col].str.strip()

                    # Adicionar o DataFrame à lista
                    dfs.append(df)
                    print(f"Arquivo processado com sucesso: {file_name} (codificação: {encoding}, delimitador: {delimiter})")
                    processed = True
                    break  # Se o arquivo for carregado com sucesso, sair do loop
                except pd.errors.ParserError:
                    print(f"Erro ao processar o arquivo {file_name} com codificação {encoding} e delimitador {delimiter}: erro de parsing.")
                except Exception as e:
                    print(f"Erro ao processar o arquivo {file_name} com codificação {encoding} e delimitador {delimiter}: {e}")
            if processed:
                break
        
        if not processed:
            print(f"Não foi possível processar o arquivo {file_name} com as combinações de codificação e delimitador testadas.")

    # Concatenar todos os DataFrames
    if dfs:
        all_data = pd.concat(dfs, ignore_index=True)
        
        # Ajustar a coluna de data_competencia para garantir o formato yyyy-mm
        all_data['data_competencia'] = pd.to_datetime(all_data['data_competencia'], format='%Y-%m').dt.to_period('M').astype(str)
        
        # Salvar em CSV
        csv_file = os.path.join(download_dir, f"{output_prefix}.csv")
        all_data.to_csv(csv_file, index=False)
        print(f"Arquivo CSV '{csv_file}' criado com sucesso.")
        
        # Salvar em Parquet
        parquet_file = os.path.join(download_dir, f"{output_prefix}.parquet")
        try:
            all_data.to_parquet(parquet_file, index=False)
            print(f"Arquivo Parquet '{parquet_file}' criado com sucesso.")
        except ImportError as e:
            print(f"Erro ao salvar o arquivo Parquet: {e}")
    else:
        print(f"Nenhum arquivo '{file_filter}' encontrado para concatenar.")

# Função para filtrar e salvar os dados dos últimos 3 meses
def filter_last_three_months(file_name, output_prefix):
    file_path = os.path.join(download_dir, file_name)
    
    # Carregar o DataFrame
    try:
        df = pd.read_csv(file_path)
        print(f"Arquivo carregado com sucesso: {file_name}")
    except Exception as e:
        print(f"Erro ao carregar o arquivo {file_name}: {e}")
        return
    
    # Ajustar a coluna de data_competencia para garantir o formato yyyy-mm
    df['data_competencia'] = pd.to_datetime(df['data_competencia'], format='%Y-%m').dt.to_period('M').astype(str)
    
    # Converter a coluna de data_competencia para datetime
    df['data_competencia'] = pd.to_datetime(df['data_competencia'], format='%Y-%m')
    
    # Obter a data atual
    today = datetime.now()
    
    # Calcular a data do início do período de 3 meses atrás
    three_months_ago = today - pd.DateOffset(months=3)
    
    # Filtrar os dados para os últimos 3 meses
    df_filtered = df[df['data_competencia'] >= three_months_ago]
    
    # Verificar se há dados após o filtro
    if not df_filtered.empty:
        # Aplicar trim nas colunas de texto
        text_columns = df_filtered.select_dtypes(include=['object']).columns
        for col in text_columns:
            df_filtered[col] = df_filtered[col].str.strip()
        
        # Ajustar a coluna de data_competencia para garantir o formato yyyy-mm
        df_filtered['data_competencia'] = df_filtered['data_competencia'].dt.to_period('M').astype(str)
        
        # Salvar em CSV
        csv_file = os.path.join(download_dir, f"{output_prefix}_últimos_3_meses.csv")
        df_filtered.to_csv(csv_file, index=False)
        print(f"Arquivo CSV '{csv_file}' criado com sucesso.")
        
        # Salvar em Parquet
        parquet_file = os.path.join(download_dir, f"{output_prefix}_últimos_3_meses.parquet")
        try:
            df_filtered.to_parquet(parquet_file, index=False)
            print(f"Arquivo Parquet '{parquet_file}' criado com sucesso.")
        except ImportError as e:
            print(f"Erro ao salvar o arquivo Parquet: {e}")
    else:
        print(f"Nenhum dado encontrado nos últimos 3 meses para o arquivo {file_name}.")

# Processar arquivos originais
process_files('horarios_', 'todos_horarios_ordenados')
process_files('linhas_secoes_', 'empresas_linhas_secoes_ordenados')

# Filtrar e salvar os dados dos últimos 3 meses
filter_last_three_months('todos_horarios_ordenados.csv', 'todos_horarios')
filter_last_three_months('empresas_linhas_secoes_ordenados.csv', 'empresas_linhas_secoes')
