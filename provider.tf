provider "aws" {
    region = "us-east-1"

    default_tags {
      tags = {
        costcenter = "data-platform"
        serviceline = "etl"
      }
    }
}