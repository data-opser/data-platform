resource "google_service_account" "env_sa" {
  account_id   = "composer3-prod"
  display_name = "Composer 3 service account"
}


resource "google_project_iam_member" "editor_sa" {
  project = var.project
  role    = "roles/editor"
  member  = google_service_account.env_sa.email
}