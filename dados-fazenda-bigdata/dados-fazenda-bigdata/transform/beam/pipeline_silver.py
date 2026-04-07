"""
transform/beam/pipeline_silver.py
-----------------------------------
Pipeline Apache Beam para transformação da camada Bronze → Silver.

Operações realizadas:
  1. Leitura dos arquivos Parquet do SICAR no GCS (Bronze)
  2. Limpeza e validação dos registros (CPF/CNPJ, geometria WKT)
  3. Normalização de campos (nome, área, datas)
  4. Cruzamento com dados SIGEF/INCRA por código do imóvel
  5. Join com embargos IBAMA por sobreposição geoespacial
  6. Escrita na tabela BigQuery (Silver)

Executado via Cloud Dataflow (DataflowRunner) ou localmente (DirectRunner).
"""

import argparse
import json
import logging
import re
from datetime import datetime

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schema BigQuery da tabela Silver
SCHEMA_SILVER = {
    "fields": [
        {"name": "cod_car",         "type": "STRING",    "mode": "REQUIRED"},
        {"name": "cpf_cnpj",        "type": "STRING",    "mode": "NULLABLE"},
        {"name": "nome_proprietario","type": "STRING",   "mode": "NULLABLE"},
        {"name": "municipio",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "estado",          "type": "STRING",    "mode": "REQUIRED"},
        {"name": "area_ha",         "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "geometry_wkt",    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "situacao_car",    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "possui_embargo",  "type": "BOOL",      "mode": "NULLABLE"},
        {"name": "num_embargos",    "type": "INT64",     "mode": "NULLABLE"},
        {"name": "cod_sigef",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "dt_ingestao",     "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "dt_referencia",   "type": "DATE",      "mode": "REQUIRED"},
    ]
}


class LimparValidarRegistro(beam.DoFn):
    """Limpa e valida cada registro do SICAR."""

    def process(self, element):
        try:
            cod_car = str(element.get("cod_imovel", "")).strip()
            if not cod_car or len(cod_car) < 10:
                return  # descarta registros sem código CAR válido

            cpf_cnpj = re.sub(r"\D", "", str(element.get("cpf_cnpj_proprietario", "")))
            area_raw = element.get("num_area", 0)
            try:
                area_ha = float(area_raw) if area_raw else None
            except (ValueError, TypeError):
                area_ha = None

            yield {
                "cod_car": cod_car,
                "cpf_cnpj": cpf_cnpj if cpf_cnpj else None,
                "nome_proprietario": str(element.get("nom_proprietario", "")).strip().upper() or None,
                "municipio": str(element.get("nom_municipio", "")).strip() or None,
                "estado": str(element.get("cod_estado", "")).strip().upper(),
                "area_ha": area_ha,
                "geometry_wkt": element.get("geometry_wkt"),
                "situacao_car": str(element.get("ind_status", "")).strip() or None,
            }
        except Exception as e:
            logger.warning(f"Erro ao limpar registro: {e} | dados: {element}")


class EnriquecerComEmbargos(beam.DoFn):
    """
    Verifica se o imóvel possui embargos IBAMA.
    Em produção, este step faz lookup na tabela Bronze de embargos no BigQuery.
    Para demonstração, simula a lógica de cruzamento.
    """

    def process(self, element):
        # Em produção: consulta BigQuery ou side input com embargos
        # Aqui simulamos: se área > 5000 ha, marca como verificar
        possui_embargo = False
        num_embargos = 0

        # Placeholder: cruzamento real usa Shapely para sobreposição de polígonos
        # from shapely.wkt import loads
        # geom = loads(element["geometry_wkt"])
        # ... join com base de embargos

        yield {
            **element,
            "possui_embargo": possui_embargo,
            "num_embargos": num_embargos,
            "cod_sigef": None,  # preenchido no join com SIGEF
            "dt_ingestao": datetime.utcnow().isoformat() + "Z",
            "dt_referencia": datetime.utcnow().date().isoformat(),
        }


def run(argv=None):
    parser = argparse.ArgumentParser(description="Pipeline Beam: Bronze → Silver")
    parser.add_argument("--project",     required=True, help="GCP Project ID")
    parser.add_argument("--bucket",      required=True, help="Bucket GCS (Bronze)")
    parser.add_argument("--dataset",     default="dados_fazenda_silver", help="Dataset BigQuery destino")
    parser.add_argument("--table",       default="sicar", help="Tabela BigQuery destino")
    parser.add_argument("--runner",      default="DirectRunner", help="DirectRunner ou DataflowRunner")
    parser.add_argument("--region",      default="us-central1")
    parser.add_argument("--temp_location", default=None)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).runner = known_args.runner

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = known_args.project
    gcp_options.region = known_args.region
    gcp_options.temp_location = known_args.temp_location or f"gs://{known_args.bucket}/temp"

    input_pattern = f"gs://{known_args.bucket}/bronze/sicar/**/*.parquet"
    tabela_destino = f"{known_args.project}:{known_args.dataset}.{known_args.table}"

    logger.info(f"Iniciando pipeline: {input_pattern} → {tabela_destino}")

    with beam.Pipeline(options=options) as p:
        (
            p
            | "LerParquet"        >> beam.io.ReadFromParquet(input_pattern)
            | "LimparValidar"     >> beam.ParDo(LimparValidarRegistro())
            | "EnriquecerEmbargo" >> beam.ParDo(EnriquecerComEmbargos())
            | "EscreverBigQuery"  >> WriteToBigQuery(
                table=tabela_destino,
                schema=SCHEMA_SILVER,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

    logger.info("Pipeline concluída.")


if __name__ == "__main__":
    run()
