resource "google_service_account" "env_sa" {
  account_id   = "composer3-prod"
  display_name = "Composer 3 service account"
}

resource "google_project_iam_member" "env_sa_worker" {
  project = var.project
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.env_sa.email}"
}

# resource "google_service_account_iam_member" "agent_can_impersonate" {
#   service_account_id = google_service_account.env_sa.name
#   role               = "roles/iam.serviceAccountUser"
#   member             = "serviceAccount:service-data-platform-457606@cloudcomposer-accounts.iam.gserviceaccount.com"
# }
