module "simple-composer-environment" {
  source  = "terraform-google-modules/composer/google//modules/create_environment_v3"
  version = "~> 6.0"

  project_id               = var.project
  composer_env_name        = "dataops"
  region                   = var.region
  composer_service_account = google_service_account.env_sa.email
  network                  = "default"
  subnetwork               = "default"
  storage_bucket           = google_storage_bucket.composer_bucket.url

  environment_size = "ENVIRONMENT_SIZE_SMALL"

  resilience_mode = "STANDARD_RESILIENCE"

  depends_on = [
    google_project_iam_member.editor_sa,
  ]
}
