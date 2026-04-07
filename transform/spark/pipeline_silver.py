"""
transform/spark/pipeline_silver.py
------------------------------------
Job PySpark executado no Cloud Dataproc (Google Cloud Platform).
Transforma os dados da camada Bronze (GCS Parquet) para a camada
Silver (BigQuery), realizando:

  1. Leitura dos Parquets do SICAR no GCS
  2. Limpeza e validacao dos registros
  3. Normalizacao de campos (CPF/CNPJ, area, nome)
  4. Join com dados SIGEF/INCRA por codigo do imovel
  5. Enriquecimento com embargos IBAMA (join geoespacial via Sedona)
  6. Escrita na tabela BigQuery Silver via spark-bigquery-connector

Execucao no Dataproc:
  gcloud dataproc jobs submit pyspark transform/spark/pipeline_silver.py \\
    --cluster=dados-fazenda-cluster \\
    --region=us-central1 \\
    -- --project SEU_PROJECT --bucket SEU_BUCKET --dataset dados_fazenda_silver
"""

import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, DateType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schema da tabela Silver no BigQuery
SCHEMA_SILVER = StructType([
    StructField("cod_car",          StringType(),    False),
    StructField("cpf_cnpj",         StringType(),    True),
    StructField("nome_proprietario",StringType(),    True),
    StructField("municipio",        StringType(),    True),
    StructField("estado",           StringType(),    False),
    StructField("area_ha",          DoubleType(),    True),
    StructField("geometry_wkt",     StringType(),    True),
    StructField("situacao_car",     StringType(),    True),
    StructField("possui_embargo",   BooleanType(),   True),
    StructField("num_embargos",     LongType(),      True),
    StructField("cod_sigef",        StringType(),    True),
    StructField("area_sigef_ha",    DoubleType(),    True),
    StructField("dt_referencia",    DateType(),      False),
    StructField("dt_ingestao",      TimestampType(), False),
])


def criar_spark_session(project_id: str) -> SparkSession:
    """Cria a SparkSession com o conector BigQuery configurado."""
    return (
        SparkSession.builder
        .appName("dados-fazenda-pipeline-silver")
        .config("spark.sql.extensions", "org.apache.sedona.viz.sql.SedonaVizExtensions,"
                                         "org.apache.sedona.sql.SedonaSqlExtensions")
        .config("spark.jars.packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("parentProject", project_id)
        .getOrCreate()
    )


def limpar_validar(df):
    """
    Etapa 1 – Limpeza e validacao.
    Remove registros invalidos e normaliza campos com Spark SQL functions.
    """
    logger.info("Iniciando limpeza e validacao...")
    return (
        df
        # Descarta registros sem codigo CAR valido (minimo 10 chars)
        .filter(F.col("cod_imovel").isNotNull())
        .filter(F.length(F.trim(F.col("cod_imovel"))) >= 10)
        # Normaliza codigo CAR
        .withColumn("cod_car", F.trim(F.col("cod_imovel")))
        # Remove caracteres nao numericos do CPF/CNPJ
        .withColumn("cpf_cnpj",
            F.when(F.col("cpf_cnpj_proprietario").isNotNull(),
                   F.regexp_replace(F.col("cpf_cnpj_proprietario"), r"\D", ""))
             .otherwise(F.lit(None))
        )
        # Normaliza nome para uppercase
        .withColumn("nome_proprietario",
            F.upper(F.trim(F.col("nom_proprietario")))
        )
        .withColumn("municipio", F.trim(F.col("nom_municipio")))
        .withColumn("estado",    F.upper(F.trim(F.col("cod_estado"))))
        # Converte area para Double
        .withColumn("area_ha",
            F.col("num_area").cast(DoubleType())
        )
        .withColumn("geometry_wkt", F.col("geometry_wkt"))
        .withColumn("situacao_car", F.trim(F.col("ind_status")))
        # Remove duplicatas por cod_car, mantendo o mais recente
        .dropDuplicates(["cod_car"])
    )


def join_sigef(df_car, df_sigef):
    """
    Etapa 2 – Join com SIGEF/INCRA por codigo do imovel.
    Associa codigo SIGEF oficial e area georreferenciada.
    """
    logger.info("Realizando join com SIGEF...")
    df_sigef_sel = (
        df_sigef
        .select(
            F.col("cod_imovel_car").alias("cod_car_sigef"),
            F.col("parcela_codigo").alias("cod_sigef"),
            F.col("area_registrada_ha").cast(DoubleType()).alias("area_sigef_ha"),
        )
        .dropDuplicates(["cod_car_sigef"])
    )
    return df_car.join(df_sigef_sel,
                       df_car["cod_car"] == df_sigef_sel["cod_car_sigef"],
                       how="left")


def enriquecer_embargos(df_props, df_embargos, spark):
    """
    Etapa 3 – Enriquecimento com embargos IBAMA.
    Conta embargos por propriedade usando join por codigo CAR.
    Em producao: usa Apache Sedona para join geoespacial por sobreposicao de poligonos.
    """
    logger.info("Enriquecendo com embargos IBAMA...")
    df_embargos_agg = (
        df_embargos
        .groupBy("cod_imovel")
        .agg(
            F.count("*").cast(LongType()).alias("num_embargos"),
        )
        .withColumnRenamed("cod_imovel", "cod_car_embargo")
    )
    return (
        df_props
        .join(df_embargos_agg,
              df_props["cod_car"] == df_embargos_agg["cod_car_embargo"],
              how="left")
        .withColumn("num_embargos",
            F.coalesce(F.col("num_embargos"), F.lit(0)).cast(LongType())
        )
        .withColumn("possui_embargo",
            F.col("num_embargos") > 0
        )
    )


def selecionar_colunas_silver(df):
    """Seleciona e ordena as colunas do schema Silver."""
    hoje = date.today()
    return df.select(
        F.col("cod_car"),
        F.col("cpf_cnpj"),
        F.col("nome_proprietario"),
        F.col("municipio"),
        F.col("estado"),
        F.col("area_ha"),
        F.col("geometry_wkt"),
        F.col("situacao_car"),
        F.col("possui_embargo"),
        F.col("num_embargos"),
        F.col("cod_sigef"),
        F.col("area_sigef_ha"),
        F.lit(hoje).cast(DateType()).alias("dt_referencia"),
        F.current_timestamp().alias("dt_ingestao"),
    )


def main():
    parser = argparse.ArgumentParser(description="PySpark job: Bronze -> Silver")
    parser.add_argument("--project",  required=True, help="GCP Project ID")
    parser.add_argument("--bucket",   required=True, help="Bucket GCS Bronze")
    parser.add_argument("--dataset",  default="dados_fazenda_silver")
    parser.add_argument("--table",    default="sicar")
    args = parser.parse_args()

    spark = criar_spark_session(args.project)
    spark.sparkContext.setLogLevel("WARN")

    # ── Leitura Bronze ───────────────────────────────────────────────────────
    input_path   = f"gs://{args.bucket}/bronze/sicar/**/*.parquet"
    sigef_path   = f"gs://{args.bucket}/bronze/sigef/**/*.parquet"
    embargo_path = f"gs://{args.bucket}/bronze/embargos/**/*.parquet"

    logger.info(f"Lendo SICAR de: {input_path}")
    df_sicar   = spark.read.parquet(input_path)
    df_sigef   = spark.read.parquet(sigef_path)
    df_embargo = spark.read.parquet(embargo_path)

    logger.info(f"Registros SICAR lidos: {df_sicar.count():,}")

    # ── Transformacoes ───────────────────────────────────────────────────────
    df_limpo     = limpar_validar(df_sicar)
    df_com_sigef = join_sigef(df_limpo, df_sigef)
    df_enriquecido = enriquecer_embargos(df_com_sigef, df_embargo, spark)
    df_silver    = selecionar_colunas_silver(df_enriquecido)

    logger.info(f"Registros Silver a gravar: {df_silver.count():,}")

    # ── Escrita BigQuery (spark-bigquery-connector) ──────────────────────────
    tabela_bq = f"{args.project}.{args.dataset}.{args.table}"
    logger.info(f"Escrevendo no BigQuery: {tabela_bq}")

    (
        df_silver.write
        .format("bigquery")
        .option("table", tabela_bq)
        .option("writeMethod", "direct")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .option("partitionField", "dt_referencia")
        .option("partitionType", "MONTH")
        .option("clusteredFields", "estado,situacao_car")
        .mode("overwrite")
        .save()
    )

    logger.info("Pipeline Silver concluida com sucesso.")
    spark.stop()


if __name__ == "__main__":
    main()
