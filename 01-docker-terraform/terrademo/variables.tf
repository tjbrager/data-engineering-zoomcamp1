variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "de-course-485518"
}


variable "location" {
  description = "Project Location"
  default     = "US"
}


variable "region" {
  description = "Project Region"
  default     = "us-central1"
}


variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}


variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "de-course-485518-terraform-bucket"
}


variable "gcs_storage_name" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}