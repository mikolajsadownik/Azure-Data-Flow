resource "azurerm_resource_group" "rg_azure_data_flow" {
  name     = var.resource_group_name
  location = var.region
}

resource "azurerm_eventhub_namespace" "eventhub_namespace" {
  name                = var.eventhub_namespace_name
  location            = azurerm_resource_group.rg_azure_data_flow.location
  resource_group_name = azurerm_resource_group.rg_azure_data_flow.name
  sku                 = "Standard"
  capacity            = 1

  tags = {
    environment = "Production"
  }
}

resource "azurerm_eventhub" "eventhub" {
  name              = var.eventhub_name
  namespace_id      = azurerm_eventhub_namespace.eventhub_namespace.id
  partition_count   = 1
  message_retention = 2
}

# create SendPolicy and ListenPolicy for eventhub
resource "azurerm_eventhub_authorization_rule" "send_policy" {
  name                = "SendPolicy"
  namespace_name      = azurerm_eventhub_namespace.eventhub_namespace.name
  eventhub_name       = azurerm_eventhub.eventhub.name
  resource_group_name = azurerm_resource_group.rg_azure_data_flow.name
  listen              = false
  send                = true
  manage              = false
}

resource "azurerm_eventhub_authorization_rule" "listen_policy" {
  name                = "ListenPolicy"
  namespace_name      = azurerm_eventhub_namespace.eventhub_namespace.name
  eventhub_name       = azurerm_eventhub.eventhub.name
  resource_group_name = azurerm_resource_group.rg_azure_data_flow.name
  listen              = true
  send                = false
  manage              = false
}

resource "azurerm_data_factory" "adf" {
  name                = var.adf_name
  location            = azurerm_resource_group.rg_azure_data_flow.location
  resource_group_name = azurerm_resource_group.rg_azure_data_flow.name
}

resource "azurerm_storage_account" "storage_account" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg_azure_data_flow.name
  location                 = azurerm_resource_group.rg_azure_data_flow.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled = true
  tags = {
    environment = "dev"
  }
}