terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = "../keys/my-creds.json"
  project = "de-course-485518"
  region  = "us-central1"
}