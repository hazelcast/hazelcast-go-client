# Azure Discovery

This project discovers Hazelcast instances running within your Azure resource group automatically or with little configuration.

## Getting Started

Azure Discovery uses [Azure Instance Metadata Service](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service) to get the access token and other environment details.
In order to use this service, Azure Managed Identities must be enabled for all VMs that runs the Go client with the following permission:
* Scope: Resource Group
* Resource Name: YOUR RESOURCE GROUP NAME
* Role: Reader

## Configuration

### Basic Configuration

In order to enable Azure discovery, `Enabled` setting must be set to true in  `AzureConfig`:
```go
config := hazelcast.NewConfig()
config.ClusterConfig.AzureConfig.Enabled = true
```
Necessary information such as subscription ID and and resource group name will be retrieved from Instance Metadata Service. Using this method, there is no need to keep any secret or password in the code or configuration.

### Additional Configuration Items

You can use the following additional configuration items when using automatic discovery:
* `Tag`: Specify a tag in the `name=value` format to filter VM instances by. If `Tag` is empty, all VM instances found in the resource group will be used.
* `HzPort`: Specify a port range for the Hazelcast instances in the `START-END` format. It is  `5701-5703` by default.

### Configuration for Outside Azure

Hazelcast client instances might be running outside of an Azure VM which makes Azure Instance Metadata service unavailable. Then, the client should be configured with the properties as shown below:
```go
config := NewConfig()
az := &config.ClusterConfig.AzureConfig
az.Enabled = true
az.InstanceMetadataAvailable = false
az.UsePublicIP = true
az.ClientID = "CLIENT_ID"
az.ClientSecret = "CLIENT_SECRET"
az.TenantID = "TENANT_ID"
az.SubscriptionID = "SUBSCRIPTION_ID"
az.ResourceGroup = "RESOURCE_GROUP_NAME"
az.ScaleSet = "SCALE_SET_NAME"
```
