variable "aws_region" {
  description = "AWS region in which the portfolio lab is deployed."
  type        = string
  default     = "ap-northeast-2"

  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9]+$", var.aws_region))
    error_message = "aws_region must look like ap-northeast-2."
  }
}

variable "project_name" {
  description = "Lowercase project identifier used in resource names."
  type        = string
  default     = "factory-sensor-poc"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,29}$", var.project_name))
    error_message = "project_name must be 3-30 characters of lowercase letters, numbers, or hyphens."
  }
}

variable "environment" {
  description = "Logical environment label. This lab should normally use portfolio."
  type        = string
  default     = "portfolio"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,15}$", var.environment))
    error_message = "environment must be 2-16 characters of lowercase letters, numbers, or hyphens."
  }
}

variable "bucket_name" {
  description = "Optional globally unique S3 bucket name. Null creates one with account and random suffix."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.bucket_name == null || (
      length(var.bucket_name) >= 3 &&
      length(var.bucket_name) <= 63 &&
      can(regex("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", var.bucket_name))
    )
    error_message = "bucket_name must be a valid 3-63 character lowercase S3 bucket name."
  }
}

variable "firehose_name" {
  description = "Optional Firehose delivery stream name. Null creates a unique project name."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.firehose_name == null || (
      length(var.firehose_name) >= 1 &&
      length(var.firehose_name) <= 64 &&
      can(regex("^[A-Za-z0-9_.-]+$", var.firehose_name))
    )
    error_message = "firehose_name must contain only letters, numbers, '.', '_' or '-'."
  }
}

variable "enable_parquet_conversion" {
  description = "Convert incoming JSON records to Snappy-compressed Parquet in Firehose."
  type        = bool
  default     = true
}

variable "firehose_buffer_interval_seconds" {
  description = "Firehose delivery interval. Conversion mode requires at least 60 seconds."
  type        = number
  default     = 60

  validation {
    condition     = var.firehose_buffer_interval_seconds >= 60 && var.firehose_buffer_interval_seconds <= 900
    error_message = "firehose_buffer_interval_seconds must be between 60 and 900 seconds."
  }
}

variable "data_retention_days" {
  description = "Lifecycle retention for bronze and error objects. Short retention limits portfolio spend."
  type        = number
  default     = 7

  validation {
    condition     = var.data_retention_days >= 1 && var.data_retention_days <= 365
    error_message = "data_retention_days must be between 1 and 365."
  }
}

variable "cloudwatch_log_retention_days" {
  description = "Retention for Firehose delivery logs."
  type        = number
  default     = 7

  validation {
    condition = contains([
      1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180,
      365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653,
    ], var.cloudwatch_log_retention_days)
    error_message = "cloudwatch_log_retention_days must be a supported CloudWatch Logs retention value."
  }
}

variable "force_destroy" {
  description = "Allow Terraform to delete non-empty S3 buckets. Keep false unless cleanup is intentional."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Additional tags applied to all supported resources."
  type        = map(string)
  default     = {}
}
