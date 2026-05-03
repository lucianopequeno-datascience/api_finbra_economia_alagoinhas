import os
import sys
import requests
import pandas as pd
from google.cloud import storage

# Configurações
BUCKET_NOME = os.getenv("BUCKET_NOME")
ROOT_PATH = os.getenv("ROOT_PATH", "financas")
MUNICIPIO_IBGE = os.getenv("MUNICIPIO_IBGE", "2900702")
ANO_INICIO = int(os.getenv("ANO_INICIO", "2015"))
ANO_FIM = int(os.getenv("ANO_FIM", "2027"))

API_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/finbra"

def fetch_finbra_ano(ano):
    print(f"  > Buscando FINBRA (Ano: {ano})...")
    
    # Parâmetros corretos para o endpoint FINBRA (DCA)
    params = {
        'id_ente': MUNICIPIO_IBGE, 
        'exercicio': ano, 
        'esfera': 'M', 
        'format': 'json'
    }
    
    try:
        response = requests.get(API_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json().get('items', [])
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        print(f"    ! Erro ao buscar ano {ano}: {e}")
        return pd.DataFrame()

def main():
    print(f"🚀 Iniciando extração histórica: {ANO_INICIO} a {ANO_FIM}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NOME)

    for ano in range(ANO_INICIO, ANO_FIM + 1):
        df = fetch_finbra_ano(ano)
        
        if not df.empty:
            csv_name = f"finbra_{MUNICIPIO_IBGE}_{ano}.csv"
            local_path = f"/tmp/{csv_name}"
            df.to_csv(local_path, index=False)
            
            # Particionamento Hive: ano={ano}
            blob_path = f"{ROOT_PATH}/landing/siconfi/finbra/ano={ano}/{csv_name}"
            bucket.blob(blob_path).upload_from_filename(local_path)
            
            print(f"✅ Ano {ano} salvo com {len(df)} linhas.")
            os.remove(local_path)
        else:
            print(f"⚠️ Nenhum dado encontrado para o ano {ano}. Continuando...")

if __name__ == "__main__":
    main()
