terraform {
  backend "gcs" {
    bucket  = "us-central1-prod-airflow-8d2312da-bucket"
    prefix  = "terraform/state"
  }
}