module "pg" {
  source  = "terraform-google-modules/sql-db/google//modules/postgresql"
  version = "~> 25.2"

  name                 = "superset"
  random_instance_name = true
  project_id           = "data-platform-457606"
  database_version     = "POSTGRES_17"

  // Master configurations
  tier                            = "db-perf-optimized-N-2"
  availability_type               = "REGIONAL"
  maintenance_window_day          = 7
  maintenance_window_hour         = 12
  maintenance_window_update_track = "stable"

  deletion_protection = false

  ip_configuration = {
    ipv4_enabled       = true
    ssl_mode           = "ENCRYPTED_ONLY"
    private_network    = null
    allocated_ip_range = null
    authorized_networks = [
        {
        name  = "public-access"
        value = "0.0.0.0/0"
        },
    ]
  }


  db_name      = "superset"
  db_charset   = "UTF8"
  db_collation = "en_US.UTF8"

  user_name     = "admin"
}

output "password" {
  sensitive = true
  value = module.pg.generated_user_password
}