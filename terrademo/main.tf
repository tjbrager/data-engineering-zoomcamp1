terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project = "de-course-485518"
  region  = "us-central1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "de-course-485518-terraform-bucket"
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