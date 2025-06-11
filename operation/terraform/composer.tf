# resource "google_composer_environment" "prod_airflow" {
#   name   = "prod-airflow"
#   region = "us-central1"
#
#   storage_config {
#     bucket = google_storage_bucket.composer_bucket.name
#   }
#
#   config {
#
#     software_config {
#       image_version  = "composer-2.13.2-airflow-2.10.5"
#       env_variables  = { ENV = "prod" }
#       airflow_config_overrides = {
#         "core-load_example" = "False"
#       }
#       pypi_packages = {
#         "dbt-bigquery"      = "1.9.2",
#         "dlt"               = "1.11.0",
#         "astronomer-cosmos" = "1.10.1"
#       }
#     }
#
#     workloads_config {
#       scheduler {
#         cpu        = 2
#         memory_gb  = 4
#         storage_gb = 5
#         count      = 1
#       }
#       web_server {
#         cpu        = 2
#         memory_gb  = 4
#         storage_gb = 5
#       }
#       worker {
#         cpu        = 2
#         memory_gb  = 4
#         storage_gb = 5
#         min_count  = 2
#         max_count  = 6
#       }
#     }
#
#     node_config {
#       service_account = "admin-340@data-platform-457606.iam.gserviceaccount.com"
#     }
#   }
# }

resource "google_composer_environment" "cc3" {
  provider = google-beta
  name     = "prod-airflow"
  region   = var.region

  storage_config {
    bucket = google_storage_bucket.composer_bucket.name
  }

  config {
    software_config {
      image_version = "composer-3-airflow-2.10.5"
      pypi_packages = {
        "dbt-bigquery" = "1.9.2"
        "dlt"          = "1.11.0"
        "astronomer-cosmos" = "1.10.1"
      }
    }

    node_config {
      service_account = google_service_account.env_sa.email
    }
#
#     environment_preset = "ENVIRONMENT_PRESET_MEDIUM"
  }
}