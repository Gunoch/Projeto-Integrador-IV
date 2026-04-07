"""
functions/farm_scan/main.py
----------------------------
Cloud Function que gera o relatório mensal Farm Scan em PDF
para cada assinante do plano Full.

Acionada pelo Cloud Scheduler todo dia 1º às 06:00 BRT.
Gera o PDF com ReportLab e envia via WhatsApp Business API.
"""

import io
import json
import logging
import os
from datetime import datetime

import requests
from google.cloud import bigquery, storage
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BQ_PROJECT  = os.environ.get("GCP_PROJECT")
BQ_DATASET  = os.environ.get("BQ_DATASET", "dados_fazenda_gold")
GCS_BUCKET  = os.environ.get("GCS_BUCKET", "dados-fazenda-reports")
WA_API_URL  = os.environ.get("WA_API_URL", "https://graph.facebook.com/v18.0")
WA_TOKEN    = os.environ.get("WA_TOKEN", "")
WA_PHONE_ID = os.environ.get("WA_PHONE_ID", "")

VERDE = colors.HexColor("#1A6B3C")
CINZA = colors.HexColor("#F5F5F5")


def buscar_propriedades_monitoradas() -> list:
    """Retorna todas as propriedades com monitoramento Full ativo."""
    cliente = bigquery.Client(project=BQ_PROJECT)
    query = f"""
        SELECT
            m.cod_car,
            m.telefone_whatsapp,
            m.nome_usuario,
            p.nome_proprietario,
            p.municipio,
            p.estado,
            p.area_car_ha,
            p.situacao_car,
            p.possui_embargo,
            p.num_embargos,
            p.dt_referencia
        FROM `{BQ_PROJECT}.{BQ_DATASET}.monitoramentos` m
        JOIN `{BQ_PROJECT}.{BQ_DATASET}.consulta_propriedade` p
          ON m.cod_car = p.cod_car
        WHERE m.plano = 'FULL'
          AND m.ativo = TRUE
        ORDER BY m.nome_usuario
    """
    resultados = cliente.query(query).result()
    return [dict(row) for row in resultados]


def gerar_pdf(prop: dict, mes_ref: str) -> bytes:
    """Gera o Farm Scan em PDF para uma propriedade e retorna os bytes."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                            leftMargin=2*cm, rightMargin=2*cm,
                            topMargin=2*cm, bottomMargin=2*cm)
    styles = getSampleStyleSheet()
    elementos = []

    # Cabeçalho
    titulo_style = ParagraphStyle("titulo", parent=styles["Heading1"],
                                  textColor=VERDE, fontSize=18)
    elementos.append(Paragraph("🌾 Farm Scan – Dados Fazenda", titulo_style))
    elementos.append(Paragraph(f"Referência: {mes_ref}", styles["Normal"]))
    elementos.append(HRFlowable(width="100%", thickness=2, color=VERDE))
    elementos.append(Spacer(1, 0.5*cm))

    # Dados da propriedade
    elementos.append(Paragraph("Dados da Propriedade", styles["Heading2"]))
    dados_tabela = [
        ["Campo", "Valor"],
        ["Código CAR", prop["cod_car"]],
        ["Proprietário", prop["nome_proprietario"] or "N/I"],
        ["Município/UF", f"{prop['municipio']}/{prop['estado']}"],
        ["Área (CAR)", f"{prop['area_car_ha']:.2f} ha" if prop["area_car_ha"] else "N/I"],
        ["Situação CAR", prop["situacao_car"] or "N/I"],
        ["Última atualização", str(prop["dt_referencia"])],
    ]
    t = Table(dados_tabela, colWidths=[5*cm, 12*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), VERDE),
        ("TEXTCOLOR",  (0, 0), (-1, 0), colors.white),
        ("FONTNAME",   (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BACKGROUND", (0, 1), (-1, -1), CINZA),
        ("GRID",       (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTSIZE",   (0, 0), (-1, -1), 10),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, CINZA]),
    ]))
    elementos.append(t)
    elementos.append(Spacer(1, 0.5*cm))

    # Status de embargos
    elementos.append(Paragraph("Status Ambiental", styles["Heading2"]))
    status_embargo = "⚠️ POSSUI EMBARGO" if prop["possui_embargo"] else "✅ SEM EMBARGOS"
    cor_status = colors.red if prop["possui_embargo"] else colors.green
    status_style = ParagraphStyle("status", parent=styles["Normal"],
                                  textColor=cor_status, fontSize=13, fontName="Helvetica-Bold")
    elementos.append(Paragraph(status_embargo, status_style))
    if prop["num_embargos"]:
        elementos.append(Paragraph(f"Total de embargos registrados: {prop['num_embargos']}", styles["Normal"]))
    elementos.append(Spacer(1, 0.5*cm))

    # Rodapé
    elementos.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
    elementos.append(Paragraph(
        f"Relatório gerado automaticamente em {datetime.utcnow().strftime('%d/%m/%Y %H:%M')} UTC | "
        "Dados Fazenda – app.dadosfazenda.com.br",
        styles["Normal"]
    ))

    doc.build(elementos)
    return buffer.getvalue()


def salvar_gcs(bucket_name: str, cod_car: str, mes_ref: str, pdf_bytes: bytes) -> str:
    """Salva o PDF no GCS e retorna o URI."""
    cliente = storage.Client()
    bucket = cliente.bucket(bucket_name)
    blob_name = f"farm_scan/{mes_ref}/{cod_car}.pdf"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(pdf_bytes, content_type="application/pdf")
    return f"gs://{bucket_name}/{blob_name}"


def enviar_whatsapp_pdf(telefone: str, nome: str, pdf_url_publica: str, mes_ref: str):
    """Envia mensagem com link do Farm Scan via WhatsApp."""
    mensagem = (
        f"📊 *Farm Scan {mes_ref} – Dados Fazenda*\n\n"
        f"Olá, {nome}! Seu relatório mensal está disponível.\n\n"
        f"📥 Download: {pdf_url_publica}\n\n"
        f"Acesse também o app: https://app.dadosfazenda.com.br"
    )
    payload = {
        "messaging_product": "whatsapp",
        "to": telefone,
        "type": "text",
        "text": {"body": mensagem}
    }
    headers = {"Authorization": f"Bearer {WA_TOKEN}", "Content-Type": "application/json"}
    resp = requests.post(f"{WA_API_URL}/{WA_PHONE_ID}/messages",
                         headers=headers, json=payload, timeout=10)
    if resp.status_code != 200:
        logger.error(f"Erro WhatsApp para {telefone}: {resp.text}")


def farm_scan(request):
    """Entry point da Cloud Function HTTP (acionada pelo Cloud Scheduler)."""
    mes_ref = datetime.utcnow().strftime("%Y-%m")
    logger.info(f"Gerando Farm Scan para referência: {mes_ref}")

    propriedades = buscar_propriedades_monitoradas()
    logger.info(f"Propriedades monitoradas (plano Full): {len(propriedades)}")

    gerados, erros = 0, 0
    for prop in propriedades:
        try:
            pdf_bytes = gerar_pdf(prop, mes_ref)
            gcs_uri = salvar_gcs(GCS_BUCKET, prop["cod_car"], mes_ref, pdf_bytes)

            # URL pública (bucket deve ter ACL pública ou usar Signed URL em produção)
            pdf_url = gcs_uri.replace("gs://", "https://storage.googleapis.com/")

            enviar_whatsapp_pdf(prop["telefone_whatsapp"], prop["nome_usuario"], pdf_url, mes_ref)
            gerados += 1
            logger.info(f"Farm Scan gerado: {prop['cod_car']}")
        except Exception as e:
            logger.error(f"Erro no CAR {prop.get('cod_car')}: {e}", exc_info=True)
            erros += 1

    resultado = {"gerados": gerados, "erros": erros, "mes_ref": mes_ref}
    logger.info(f"Farm Scan concluído: {resultado}")
    return json.dumps(resultado), 200, {"Content-Type": "application/json"}
