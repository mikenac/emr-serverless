resource "aws_s3_bucket" "emr-serverless-bucket" {

    bucket = var.bucket_name
    force_destroy = true
    
    tags = {
        Name = var.bucket_name
    }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "emr-serverless-bucket-sse" {
  bucket = aws_s3_bucket.emr-serverless-bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_acl" "emr-serverless-bucket-acl" {

    bucket = aws_s3_bucket.emr-serverless-bucket.id
    acl = "private"
}

resource "aws_s3_bucket_public_access_block" "block-public" {
  bucket = aws_s3_bucket.emr-serverless-bucket.id

  block_public_acls = true
  block_public_policy = true
  
}