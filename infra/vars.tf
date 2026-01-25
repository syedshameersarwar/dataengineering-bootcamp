variable "project" {
  description = "Project"
  default     = "<Your Project ID>"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "<Your GCP Region>"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "<Your Location>"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "<Your Dataset Name>"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "<Your Bucket Name>"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}