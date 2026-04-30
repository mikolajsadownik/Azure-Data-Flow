variable "resource_group_name" {
  type        = string
  description = "Name of the Azure Resource Group"
}

variable "region" {
  type        = string
  description = "Azure region"
}
variable "eventhub_namespace_name" {
  type        = string
  description = "Eventhub namespace name"
}

variable "eventhub_name" {
  type        = string
  description = "Eventhub name"
}

variable "adf_name" {
  type        = string
  description = "Azure data factory name"
}
