import os
import sys
import requests
import pandas as pd
from google.cloud import storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configurações do Ambiente
BUCKET_NOME = os.getenv("BUCKET_NOME")
ROOT_PATH = os.getenv("ROOT_PATH", "financas")
MUNICIPIO_IBGE = os.getenv("MUNICIPIO_IBGE", "2900702")
ANO_INICIO = int(os.getenv("ANO_INICIO", "2015"))
ANO_FIM = int(os.getenv("ANO_FIM", "2027"))

API_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/finbra"

def get_session():
    session = requests.Session()
    # Retentativas robustas: 5 tentativas com backoff exponencial
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def fetch_finbra(ano, anexo):
    print(f"  > Buscando {anexo} (Ano: {ano})...")
    params = {'id_ente': MUNICIPIO_IBGE, 'exercicio': ano, 'esfera': 'M', 'anexo': anexo}
    
    try:
        response = get_session().get(API_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json().get('items', [])
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        print(f"    ! Erro ao buscar {anexo} para {ano}: {e}")
        return pd.DataFrame()

def main():
    print(f"🚀 Iniciando ingestão histórica: {ANO_INICIO} a {ANO_FIM}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NOME)
    
    anexos = ['Receitas Orçamentárias', 'Despesas Orçamentárias']

    for ano in range(ANO_INICIO, ANO_FIM + 1):
        print(f"\n--- Processando exercício: {ano} ---")
        df_completo = pd.DataFrame()
        
        for anexo in anexos:
            df = fetch_finbra(ano, anexo)
            if not df.empty:
                df_completo = pd.concat([df_completo, df], ignore_index=True)
        
        if not df_completo.empty:
            csv_name = f"finbra_{MUNICIPIO_IBGE}_{ano}.csv"
            local_path = f"/tmp/{csv_name}"
            df_completo.to_csv(local_path, index=False)
            
            # Particionamento Hive: ano={ano}
            blob_path = f"{ROOT_PATH}/landing/siconfi/finbra/ano={ano}/{csv_name}"
            bucket.blob(blob_path).upload_from_filename(local_path)
            
            print(f"✅ Ano {ano} salvo em: gs://{BUCKET_NOME}/{blob_path}")
            os.remove(local_path)
        else:
            print(f"⚠️ Nenhum dado encontrado para o ano {ano}. Continuando...")

if __name__ == "__main__":
    main()