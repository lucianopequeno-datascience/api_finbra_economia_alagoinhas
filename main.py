import os
import time
import requests
import pandas as pd
from google.cloud import storage

# ==============================
# Configurações
# ==============================
BUCKET_NOME = os.getenv("BUCKET_NOME")
ROOT_PATH = os.getenv("ROOT_PATH", "financas")
MUNICIPIO_IBGE = os.getenv("MUNICIPIO_IBGE", "2900702")
ANO_INICIO = int(os.getenv("ANO_INICIO", "2015"))
ANO_FIM = int(os.getenv("ANO_FIM", "2023"))  # ⚠️ evite anos futuros

API_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/dca"


# ==============================
# Função de extração
# ==============================
def fetch_finbra_ano(ano):
    print(f"  > Buscando DCA/FINBRA (Ano: {ano})...")

    params = {
        "id_ente": MUNICIPIO_IBGE,
        "an_exercicio": ano
    }

    try:
        response = requests.get(API_URL, params=params, timeout=60)

        if response.status_code == 404:
            print(f"⚠️ Ano {ano} não disponível na API.")
            return pd.DataFrame()

        response.raise_for_status()

        data = response.json().get("items", [])

        if not data:
            print(f"⚠️ Ano {ano} retornou vazio.")
            return pd.DataFrame()

        return pd.DataFrame(data)

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao buscar ano {ano}: {e}")
        return pd.DataFrame()


# ==============================
# Pipeline principal
# ==============================
def main():
    print(f"🚀 Iniciando extração histórica: {ANO_INICIO} a {ANO_FIM}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NOME)

    for ano in range(ANO_INICIO, ANO_FIM + 1):
        df = fetch_finbra_ano(ano)

        if not df.empty:
            csv_name = f"dca_{MUNICIPIO_IBGE}_{ano}.csv"
            local_path = f"/tmp/{csv_name}"

            df.to_csv(local_path, index=False)

            # Particionamento Hive
            blob_path = f"{ROOT_PATH}/landing/siconfi/dca/ano={ano}/{csv_name}"

            bucket.blob(blob_path).upload_from_filename(local_path)

            print(f"✅ Ano {ano} salvo com {len(df)} linhas.")

            os.remove(local_path)

        else:
            print(f"⚠️ Nenhum dado para {ano}. Pulando...")

        # ⚠️ Respeitar limite da API
        time.sleep(1)


# ==============================
# Execução
# ==============================
if __name__ == "__main__":
    main()
