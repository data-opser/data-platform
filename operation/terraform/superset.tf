module "instance_template" {
  source  = "terraform-google-modules/vm/google//modules/instance_template"
  version = "~> 13.0"

  region             = var.region
  project_id         = var.project
  network = "default"
  subnetwork = "default"
  machine_type = "e2-standard-2"
}

module "compute_instance" {
  source  = "terraform-google-modules/vm/google//modules/compute_instance"
  version = "~> 13.0"

  subnetwork_project  = var.project
  num_instances       = 1
  instance_template   = module.instance_template.self_link
  deletion_protection = false
  subnetwork = "default"
  access_config = [{
    nat_ip       = google_compute_address.static_ip_superset.address
    network_tier = "STANDARD"
  }]
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

resource "google_compute_address" "static_ip_superset" {
  name    = "superset-static-ip"
  address_type = "EXTERNAL"
  network_tier = "STANDARD"
}