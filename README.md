# 🌾 Dados Fazenda – Pipeline de Big Data

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
[Cloud Pub/Sub] ──streaming──▶ [Cloud Dataflow] ──▶ [BigQuery Silver]
        │                                                     │
[Cloud Scheduler]──batch──▶ [Cloud Functions] ──▶ [GCS Bronze]    │
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
│   ├── beam/           # Pipelines Apache Beam (processamento distribuído)
│   └── dbt/            # Transformações SQL Silver → Gold (dbt)
├── functions/
│   ├── alert_watcher/  # Monitora embargos e dispara alertas WhatsApp
│   └── farm_scan/      # Gera relatório mensal PDF (Farm Scan)
├── infra/
│   └── terraform/      # Infraestrutura como código (GCP)
├── dashboard/          # Configuração Looker Studio
└── docs/               # Diagrama de arquitetura e documentação técnica
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

### 5. Executar pipeline de transformação Beam

```bash
cd transform/beam
python pipeline_silver.py \
  --project SEU_PROJECT_ID \
  --runner DataflowRunner \
  --region us-central1 \
  --temp_location gs://SEU_BUCKET/temp
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
- [Apache Beam Docs](https://beam.apache.org/documentation)
- [dbt Docs](https://docs.getdbt.com)
- [Dados Fazenda](https://dadosfazenda.com.br)
