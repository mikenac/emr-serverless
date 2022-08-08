variable "application_name" {
    description = "EMR serverless application name"
    default = "emr-serverless-application"
}
variable "bucket_name" { 
    description = "The bucket to be created for application use."
    default = "dp-emr-serverless"
}
variable "application_max_cores" {
    description = "The maximum CPU cores for the entire application."
    default = "16 vCPU"
}
variable "application_max_memory" {
    description = "The maximum memory available for the entire application."
    default = "64 GB"
}