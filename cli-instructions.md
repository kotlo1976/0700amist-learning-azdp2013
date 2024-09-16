
# Create a Resource group
az group create -n mytestrg -l eastus

New-AzResourceGroup -Name mytestRGusingpowershell -Location eastus


# Create Data Factory
az datafactory create --location "East US" --name "ssadcloudmainadf2" --resource-group "mytestrg"

# Create Pipeline