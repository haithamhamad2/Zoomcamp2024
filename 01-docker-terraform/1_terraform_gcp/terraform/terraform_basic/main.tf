terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
# Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
  credentials = "./keys/my_cred.json"
  project = "sonic-airfoil-411903"
  region  = "us-west1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "sonic-airfoil-411903-terra-bucket"
  location      = "US"
  force_destroy = true

 
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
