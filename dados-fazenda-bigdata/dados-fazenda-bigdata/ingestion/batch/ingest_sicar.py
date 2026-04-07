"""
ingestion/batch/ingest_sicar.py
--------------------------------
Job de ingestão batch do SICAR (Cadastro Ambiental Rural).
Baixa os arquivos oficiais por estado, converte para Parquet
e armazena no Cloud Storage (camada Bronze).

Executado diariamente via Cloud Scheduler → Cloud Functions.
"""

import argparse
import logging
import os
import requests
import pandas as pd
from datetime import date
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Estados brasileiros
ESTADOS = [
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA",
    "MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN",
    "RO","RR","RS","SC","SE","SP","TO"
]

# URL base da API pública do SICAR
SICAR_BASE_URL = "https://www.car.gov.br/publico/estados/downloadBase"


def download_sicar_estado(estado: str, destino_local: str) -> str:
    """Baixa o shapefile do CAR para um estado e retorna o caminho local."""
    url = f"{SICAR_BASE_URL}?idEstado={estado}&tipoBase=CAR_VALIDADO"
    logger.info(f"Baixando SICAR: {estado} → {url}")
    response = requests.get(url, timeout=120, stream=True)
    response.raise_for_status()
    caminho = os.path.join(destino_local, f"sicar_{estado}.zip")
    with open(caminho, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return caminho


def converter_para_parquet(caminho_zip: str, estado: str) -> str:
    """Descompacta o shapefile e converte para Parquet."""
    import zipfile, tempfile
    import geopandas as gpd

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(caminho_zip, "r") as z:
            z.extractall(tmpdir)
        shp_files = [f for f in os.listdir(tmpdir) if f.endswith(".shp")]
        if not shp_files:
            raise ValueError(f"Nenhum .shp encontrado para {estado}")
        gdf = gpd.read_file(os.path.join(tmpdir, shp_files[0]))
        # Serializar geometria como WKT para armazenamento em Parquet
        gdf["geometry_wkt"] = gdf["geometry"].apply(lambda g: g.wkt if g else None)
        df = pd.DataFrame(gdf.drop(columns="geometry"))
        parquet_path = caminho_zip.replace(".zip", ".parquet")
        df.to_parquet(parquet_path, index=False)
    return parquet_path


def upload_gcs(bucket_name: str, origem: str, estado: str) -> str:
    """Faz upload do Parquet para o GCS na camada Bronze."""
    cliente = storage.Client()
    bucket = cliente.bucket(bucket_name)
    hoje = date.today().isoformat()
    destino_blob = f"bronze/sicar/dt={hoje}/estado={estado}/data.parquet"
    blob = bucket.blob(destino_blob)
    blob.upload_from_filename(origem)
    uri = f"gs://{bucket_name}/{destino_blob}"
    logger.info(f"Upload concluído: {uri}")
    return uri


def main():
    parser = argparse.ArgumentParser(description="Ingestão batch SICAR → GCS Bronze")
    parser.add_argument("--project", required=True, help="GCP Project ID")
    parser.add_argument("--bucket", required=True, help="Nome do bucket GCS")
    parser.add_argument("--estados", nargs="+", default=ESTADOS, help="Lista de estados (padrão: todos)")
    parser.add_argument("--tmp", default="/tmp", help="Diretório temporário local")
    args = parser.parse_args()

    os.makedirs(args.tmp, exist_ok=True)
    erros = []

    for estado in args.estados:
        try:
            logger.info(f"=== Processando estado: {estado} ===")
            zip_path = download_sicar_estado(estado, args.tmp)
            parquet_path = converter_para_parquet(zip_path, estado)
            upload_gcs(args.bucket, parquet_path, estado)
            os.remove(zip_path)
            os.remove(parquet_path)
        except Exception as e:
            logger.error(f"Erro no estado {estado}: {e}")
            erros.append(estado)

    if erros:
        logger.warning(f"Estados com erro: {erros}")
    else:
        logger.info("Ingestão SICAR concluída com sucesso para todos os estados.")


if __name__ == "__main__":
    main()
