# 🌾 Dados Fazenda – Solução de Big Data para Análise e Monitoramento de Propriedades Rurais

**Projeto Integrador IV-A** | PUC Goiás – Big Data e Inteligência Artificial  
**Aluno:** Gustavo Nolasco Chaves  
**Organização parceira:** Dados Fazenda / AgriOS  
**Plataforma:** Google Cloud Platform (GCP)

---

## 📋 Descrição

Solução de Big Data para ingestão, processamento distribuído e análise de dados fundiários de propriedades rurais brasileiras, utilizando dados oficiais do SICAR (CAR), SIGEF/INCRA e IBAMA.

A arquitetura segue o padrão **Medallion (Bronze → Silver → Gold)** sobre GCP.

---

## 🏗️ Arquitetura

```
[SICAR / SIGEF / IBAMA]
        │
        ▼
[Cloud Pub/Sub] ──streaming──▶ [Cloud Functions] ──▶ [BigQuery Silver]
        │                                                     │
[Cloud Scheduler]──batch──▶ [Cloud Functions] ──▶ [GCS Bronze]    │
                                                    │             │
                                              [Dataproc/Spark] ──▶│
                                                              ▼
                                                    [BigQuery Gold]
                                                              │
                                                    [Looker Studio]
```

---

## 📁 Estrutura do Repositório

```
├── ingestion/
│   ├── batch/          # Ingestão diária SICAR + SIGEF (Cloud Functions)
│   └── streaming/      # Ingestão em tempo real de embargos (Pub/Sub)
├── transform/
│   ├── beam/           # Pipeline Apache Beam (referência)
│   ├── spark/          # Pipeline PySpark/Dataproc (Bronze → Silver)
│   └── dbt/            # Transformações SQL Silver → Gold (dbt)
├── functions/
│   ├── alert_watcher/  # Monitora embargos e dispara alertas WhatsApp
│   └── farm_scan/      # Gera relatório mensal PDF (Farm Scan)
└── infra/
    └── terraform/      # Infraestrutura como código (GCP)
```

---

## ⚙️ Pré-requisitos

- Python 3.10+
- Google Cloud SDK (`gcloud`)
- Terraform 1.5+
- dbt-bigquery (`pip install dbt-bigquery`)
- Conta GCP com billing ativo

---

## 🚀 Como Executar

### 1. Configurar o ambiente GCP

```bash
gcloud auth login
gcloud config set project SEU_PROJECT_ID
```

### 2. Provisionar infraestrutura

```bash
cd infra/terraform
terraform init
terraform apply -var="project_id=SEU_PROJECT_ID"
```

### 3. Instalar dependências Python

```bash
pip install -r requirements.txt
```

### 4. Executar pipeline batch (ingestão SICAR)

```bash
cd ingestion/batch
python ingest_sicar.py --project SEU_PROJECT_ID --bucket SEU_BUCKET
```

### 5. Executar pipeline de transformação Spark (Bronze → Silver)

```bash
gcloud dataproc jobs submit pyspark transform/spark/pipeline_silver.py \
  --cluster=dados-fazenda-cluster \
  --region=us-central1 \
  -- --project SEU_PROJECT_ID --bucket SEU_BUCKET --dataset dados_fazenda_silver
```

### 6. Executar transformações dbt (Silver → Gold)

```bash
cd transform/dbt
dbt run --profiles-dir .
```

---

## 📊 Resultados

- Consulta individual de propriedade: < 2 segundos
- Monitoramento contínuo de embargos: alertas em tempo real
- Farm Scan mensal: gerado automaticamente todo dia 1º
- Dashboard operacional: Looker Studio conectado ao BigQuery

---

## 🔗 Referências

- [BigQuery Docs](https://cloud.google.com/bigquery/docs)
- [Apache Spark Docs](https://spark.apache.org/docs/latest/)
- [Cloud Dataproc Docs](https://cloud.google.com/dataproc/docs)
- [dbt Docs](https://docs.getdbt.com)
- [Dados Fazenda](https://dadosfazenda.com.br)
