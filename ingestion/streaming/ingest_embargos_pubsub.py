"""
ingestion/streaming/ingest_embargos_pubsub.py
----------------------------------------------
Publica novos registros de embargos do IBAMA no Cloud Pub/Sub
para processamento em tempo real pelo Dataflow.

Consulta a API do IBAMA a cada N minutos e publica
apenas os registros novos (controle por timestamp).
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from google.cloud import pubsub_v1, storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IBAMA_API_URL = "https://servicos.ibama.gov.br/ctf/publico/areasembargadas/ConsultaPublicaAreasEmbargadas.php"
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "projects/{project}/topics/embargos-ibama")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "dados-fazenda-bronze")
CHECKPOINT_KEY = "streaming/embargos/checkpoint.json"
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL", "300"))  # 5 min


def carregar_checkpoint(bucket_name: str) -> dict:
    """Carrega o checkpoint (último timestamp processado) do GCS."""
    cliente = storage.Client()
    bucket = cliente.bucket(bucket_name)
    blob = bucket.blob(CHECKPOINT_KEY)
    if blob.exists():
        return json.loads(blob.download_as_text())
    return {"ultimo_id": 0, "ultima_atualizacao": "1970-01-01T00:00:00Z"}


def salvar_checkpoint(bucket_name: str, checkpoint: dict):
    """Persiste o checkpoint atualizado no GCS."""
    cliente = storage.Client()
    bucket = cliente.bucket(bucket_name)
    blob = bucket.blob(CHECKPOINT_KEY)
    blob.upload_from_string(json.dumps(checkpoint), content_type="application/json")


def buscar_embargos_novos(ultimo_id: int) -> list:
    """Consulta a API do IBAMA e retorna apenas registros com ID > ultimo_id."""
    try:
        resp = requests.get(IBAMA_API_URL, timeout=30)
        resp.raise_for_status()
        dados = resp.json()
        novos = [r for r in dados if int(r.get("seq_tad", 0)) > ultimo_id]
        return novos
    except Exception as e:
        logger.error(f"Erro ao consultar IBAMA: {e}")
        return []


def publicar_pubsub(project_id: str, topic_id: str, registros: list):
    """Publica cada registro como uma mensagem no Pub/Sub."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    futuros = []
    for registro in registros:
        mensagem = json.dumps({
            **registro,
            "ingested_at": datetime.now(timezone.utc).isoformat()
        }).encode("utf-8")
        futuro = publisher.publish(topic_path, data=mensagem)
        futuros.append(futuro)
    for f in futuros:
        f.result()  # aguarda confirmação
    logger.info(f"{len(registros)} embargo(s) publicado(s) no Pub/Sub.")


def main():
    project_id = os.environ.get("GCP_PROJECT")
    topic_id = os.environ.get("PUBSUB_TOPIC_ID", "embargos-ibama")

    if not project_id:
        raise ValueError("Variável de ambiente GCP_PROJECT não definida.")

    logger.info("Iniciando monitoramento de embargos IBAMA (streaming)...")

    while True:
        checkpoint = carregar_checkpoint(GCS_BUCKET)
        ultimo_id = checkpoint["ultimo_id"]

        novos = buscar_embargos_novos(ultimo_id)

        if novos:
            publicar_pubsub(project_id, topic_id, novos)
            novo_ultimo_id = max(int(r.get("seq_tad", 0)) for r in novos)
            salvar_checkpoint(GCS_BUCKET, {
                "ultimo_id": novo_ultimo_id,
                "ultima_atualizacao": datetime.now(timezone.utc).isoformat()
            })
            logger.info(f"Checkpoint atualizado: ultimo_id={novo_ultimo_id}")
        else:
            logger.info("Nenhum embargo novo encontrado.")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
