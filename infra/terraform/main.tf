# infra/terraform/main.tf
# Provisiona toda a infraestrutura GCP do projeto Dados Fazenda Big Data

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ─────────────────────────────────────────────
# VARIÁVEIS
# ─────────────────────────────────────────────
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}
variable "region" {
  description = "Região GCP"
  type        = string
  default     = "us-central1"
}
variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# ─────────────────────────────────────────────
# CLOUD STORAGE – Data Lake (Bronze + Reports)
# ─────────────────────────────────────────────
resource "google_storage_bucket" "bronze" {
  name          = "${var.project_id}-bronze-${var.environment}"
  location      = var.region
  force_destroy = var.environment != "prod"

  lifecycle_rule {
    condition { age = 365 }
    action    { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }

  versioning { enabled = true }

  labels = {
    project     = "dados-fazenda"
    environment = var.environment
    layer       = "bronze"
  }
}

resource "google_storage_bucket" "reports" {
  name     = "${var.project_id}-reports-${var.environment}"
  location = var.region

  labels = {
    project     = "dados-fazenda"
    environment = var.environment
    layer       = "reports"
  }
}

# ─────────────────────────────────────────────
# BIGQUERY – Data Warehouse
# ─────────────────────────────────────────────
resource "google_bigquery_dataset" "silver" {
  dataset_id  = "dados_fazenda_silver"
  description = "Camada Silver – dados limpos e normalizados"
  location    = var.region

  labels = {
    project     = "dados-fazenda"
    environment = var.environment
    layer       = "silver"
  }
}

resource "google_bigquery_dataset" "gold" {
  dataset_id  = "dados_fazenda_gold"
  description = "Camada Gold – tabelas analíticas e de serving"
  location    = var.region

  labels = {
    project     = "dados-fazenda"
    environment = var.environment
    layer       = "gold"
  }
}

# ─────────────────────────────────────────────
# PUB/SUB – Mensageria de Embargos
# ─────────────────────────────────────────────
resource "google_pubsub_topic" "embargos" {
  name = "embargos-ibama"
  labels = {
    project = "dados-fazenda"
  }
}

resource "google_pubsub_subscription" "embargos_dataflow" {
  name  = "embargos-ibama-dataflow"
  topic = google_pubsub_topic.embargos.name

  ack_deadline_seconds    = 60
  retain_acked_messages   = false
  message_retention_duration = "86400s"  # 24h
}

resource "google_pubsub_subscription" "embargos_alert" {
  name  = "embargos-ibama-alert"
  topic = google_pubsub_topic.embargos.name

  ack_deadline_seconds = 30

  push_config {
    push_endpoint = google_cloudfunctions2_function.alert_watcher.service_config[0].uri
  }
}

# ─────────────────────────────────────────────
# CLOUD FUNCTIONS (2ª geração)
# ─────────────────────────────────────────────
resource "google_cloudfunctions2_function" "alert_watcher" {
  name        = "alert-watcher"
  location    = var.region
  description = "Monitora embargos e envia alertas WhatsApp"

  build_config {
    runtime     = "python311"
    entry_point = "alert_watcher"
    source {
      storage_source {
        bucket = google_storage_bucket.bronze.name
        object = "functions/alert_watcher.zip"
      }
    }
  }

  service_config {
    max_instance_count = 10
    available_memory   = "256M"
    timeout_seconds    = 60
    environment_variables = {
      GCP_PROJECT = var.project_id
      BQ_DATASET  = "dados_fazenda_gold"
      GCS_BUCKET  = google_storage_bucket.bronze.name
    }
  }
}

resource "google_cloudfunctions2_function" "farm_scan" {
  name        = "farm-scan"
  location    = var.region
  description = "Gera relatório mensal Farm Scan em PDF"

  build_config {
    runtime     = "python311"
    entry_point = "farm_scan"
    source {
      storage_source {
        bucket = google_storage_bucket.bronze.name
        object = "functions/farm_scan.zip"
      }
    }
  }

  service_config {
    max_instance_count = 5
    available_memory   = "512M"
    timeout_seconds    = 300
    environment_variables = {
      GCP_PROJECT = var.project_id
      BQ_DATASET  = "dados_fazenda_gold"
      GCS_BUCKET  = google_storage_bucket.reports.name
    }
  }
}

# ─────────────────────────────────────────────
# CLOUD SCHEDULER – Jobs Periódicos
# ─────────────────────────────────────────────
resource "google_cloud_scheduler_job" "ingestao_sicar" {
  name        = "ingestao-sicar-diaria"
  description = "Dispara ingestão batch do SICAR todo dia às 02:00 BRT"
  schedule    = "0 5 * * *"  # 02:00 BRT = 05:00 UTC
  time_zone   = "America/Sao_Paulo"
  region      = var.region

  http_target {
    uri         = "https://${var.region}-${var.project_id}.cloudfunctions.net/ingestao-sicar"
    http_method = "POST"
  }
}

resource "google_cloud_scheduler_job" "farm_scan_mensal" {
  name        = "farm-scan-mensal"
  description = "Gera Farm Scan todo dia 1º às 06:00 BRT"
  schedule    = "0 9 1 * *"  # 06:00 BRT = 09:00 UTC
  time_zone   = "America/Sao_Paulo"
  region      = var.region

  http_target {
    uri         = google_cloudfunctions2_function.farm_scan.service_config[0].uri
    http_method = "POST"
  }
}

# ─────────────────────────────────────────────
# OUTPUTS
# ─────────────────────────────────────────────
output "bucket_bronze" {
  value = google_storage_bucket.bronze.name
}
output "bucket_reports" {
  value = google_storage_bucket.reports.name
}
output "bigquery_silver" {
  value = google_bigquery_dataset.silver.dataset_id
}
output "bigquery_gold" {
  value = google_bigquery_dataset.gold.dataset_id
}
output "pubsub_topic_embargos" {
  value = google_pubsub_topic.embargos.name
}
