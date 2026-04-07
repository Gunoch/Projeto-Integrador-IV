"""
functions/alert_watcher/main.py
---------------------------------
Cloud Function acionada por mensagens do Pub/Sub (novos embargos IBAMA).
Para cada embargo recebido, verifica se alguma propriedade monitorada
(plano Full) é afetada e envia alerta via WhatsApp Business API.

Trigger: Pub/Sub topic 'embargos-ibama'
"""

import base64
import json
import logging
import os

import requests
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BQ_PROJECT   = os.environ.get("GCP_PROJECT")
BQ_DATASET   = os.environ.get("BQ_DATASET", "dados_fazenda_gold")
WA_API_URL   = os.environ.get("WA_API_URL", "https://graph.facebook.com/v18.0")
WA_TOKEN     = os.environ.get("WA_TOKEN", "")
WA_PHONE_ID  = os.environ.get("WA_PHONE_ID", "")


def buscar_proprietarios_afetados(cod_car: str) -> list:
    """
    Consulta o BigQuery para encontrar assinantes plano Full
    que monitoram a propriedade com o código CAR recebido.
    """
    cliente = bigquery.Client(project=BQ_PROJECT)
    query = f"""
        SELECT
            m.telefone_whatsapp,
            m.nome_usuario,
            p.nome_proprietario,
            p.municipio,
            p.estado
        FROM `{BQ_PROJECT}.{BQ_DATASET}.monitoramentos` m
        JOIN `{BQ_PROJECT}.{BQ_DATASET}.consulta_propriedade` p
          ON m.cod_car = p.cod_car
        WHERE m.cod_car = @cod_car
          AND m.plano = 'FULL'
          AND m.ativo = TRUE
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("cod_car", "STRING", cod_car)]
    )
    resultados = cliente.query(query, job_config=job_config).result()
    return [dict(row) for row in resultados]


def enviar_alerta_whatsapp(telefone: str, dados: dict, embargo: dict):
    """Envia mensagem de alerta de embargo via WhatsApp Business API."""
    mensagem = (
        f"⚠️ *Alerta de Embargo – Dados Fazenda*\n\n"
        f"Olá, {dados['nome_usuario']}!\n\n"
        f"A propriedade monitorada *{dados['nome_proprietario']}* "
        f"({dados['municipio']}/{dados['estado']}) "
        f"recebeu um novo embargo do IBAMA.\n\n"
        f"*Nº TAD:* {embargo.get('num_tad', 'N/A')}\n"
        f"*Tipo de infração:* {embargo.get('des_infracao', 'N/A')}\n"
        f"*Data:* {embargo.get('dat_embargo', 'N/A')}\n\n"
        f"Acesse o app para mais detalhes: https://app.dadosfazenda.com.br"
    )
    payload = {
        "messaging_product": "whatsapp",
        "to": telefone,
        "type": "text",
        "text": {"body": mensagem}
    }
    headers = {
        "Authorization": f"Bearer {WA_TOKEN}",
        "Content-Type": "application/json"
    }
    resp = requests.post(
        f"{WA_API_URL}/{WA_PHONE_ID}/messages",
        headers=headers,
        json=payload,
        timeout=10
    )
    if resp.status_code != 200:
        logger.error(f"Erro ao enviar WhatsApp para {telefone}: {resp.text}")
    else:
        logger.info(f"Alerta enviado para {telefone}")


def alert_watcher(event, context):
    """
    Entry point da Cloud Function.
    Recebe mensagem Pub/Sub com dados do embargo e processa alertas.
    """
    try:
        payload = base64.b64decode(event["data"]).decode("utf-8")
        embargo = json.loads(payload)
        cod_car = embargo.get("cod_imovel")

        if not cod_car:
            logger.warning("Embargo sem cod_imovel, ignorando.")
            return

        logger.info(f"Processando embargo para CAR: {cod_car}")
        afetados = buscar_proprietarios_afetados(cod_car)

        if not afetados:
            logger.info(f"Nenhum assinante Full monitora CAR {cod_car}.")
            return

        for usuario in afetados:
            enviar_alerta_whatsapp(usuario["telefone_whatsapp"], usuario, embargo)

        logger.info(f"{len(afetados)} alerta(s) enviado(s) para CAR {cod_car}.")

    except Exception as e:
        logger.error(f"Erro no alert_watcher: {e}", exc_info=True)
        raise
