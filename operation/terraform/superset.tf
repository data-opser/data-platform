module "instance_template" {
  source  = "terraform-google-modules/vm/google//modules/instance_template"
  version = "~> 13.0"

  region             = var.region
  project_id         = var.project
  network = "default"
  machine_type = "e2-standard-2" 
}

module "compute_instance" {
  source  = "terraform-google-modules/vm/google//modules/compute_instance"
  version = "~> 13.0"

  subnetwork_project  = var.project
  num_instances       = 1
  instance_template   = module.instance_template.self_link
  deletion_protection = false
}

resource "google_compute_firewall" "allow-trafick" {
  name    = "allow-trafick"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22", "80", "443", "8088"]  # Откройте другие, если нужно (например, 80, 443)
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["allow-internet"]
}