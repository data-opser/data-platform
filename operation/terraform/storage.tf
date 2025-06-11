resource "google_storage_bucket" "composer_bucket" {
  name     = "dataoops-prod-dags"
  location = var.region
  storage_class = "STANDARD"
}
