import os
import time
import requests
import pandas as pd
from google.cloud import storage

# ==============================
# Configurações
# ==============================
BUCKET_NOME = os.getenv("BUCKET_NOME", "dados_alagoinhas_bronze")
ROOT_PATH = os.getenv("ROOT_PATH", "financas")
MUNICIPIO_IBGE = os.getenv("MUNICIPIO_IBGE", "2900702")
ANO_INICIO = int(os.getenv("ANO_INICIO", "2015"))
ANO_FIM = int(os.getenv("ANO_FIM", "2023"))

API_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/dca"


# ==============================
# Função de extração
# ==============================
def fetch_finbra_ano(ano):
    print(f"  > Buscando DCA (Ano: {ano})...")

    params = {
        "id_ente": MUNICIPIO_IBGE,
        "an_exercicio": ano
    }

    try:
        response = requests.get(API_URL, params=params, timeout=60)

        if response.status_code == 404:
            print(f"⚠️ Ano {ano} não disponível.")
            return pd.DataFrame()

        response.raise_for_status()

        data = response.json().get("items", [])

        return pd.DataFrame(data) if data else pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro no ano {ano}: {e}")
        return pd.DataFrame()


# ==============================
# Pipeline principal
# ==============================
def main():
    print(f"🚀 Extração: {ANO_INICIO} a {ANO_FIM}")

    # 🔥 Validação importante
    if not BUCKET_NOME:
        raise ValueError("BUCKET_NOME não definido.")

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NOME)

    for ano in range(ANO_INICIO, ANO_FIM + 1):
        df = fetch_finbra_ano(ano)

        if not df.empty:
            csv_name = f"dca_{MUNICIPIO_IBGE}_{ano}.csv"
            local_path = f"/tmp/{csv_name}"

            df.to_csv(local_path, index=False)

            # 🔥 Caminho no bucket (bronze layer)
            blob_path = f"{ROOT_PATH}/bronze/siconfi/dca/ano={ano}/{csv_name}"

            bucket.blob(blob_path).upload_from_filename(local_path)

            print(f"✅ Upload OK: {blob_path}")

            os.remove(local_path)

        else:
            print(f"⚠️ Sem dados: {ano}")

        time.sleep(1)  # respeita limite da API


# ==============================
# Execução
# ==============================
if __name__ == "__main__":
    main()
