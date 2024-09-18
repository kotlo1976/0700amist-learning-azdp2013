## 1. Prepare a Container and Test File

1. **Create a Resource Group:**
    ```bash
    az group create --name mainRG --location eastus
    ```

2. **Create a Storage Account:**
    ```bash
    az storage account create --resource-group mainRG --name ssacloudadfstorage --location eastus
    ```

3. **Create a Container:**
    ```bash
    az storage container create --resource-group mainRG --name cont --account-name ssacloudadfstorage --auth-mode key
    ```

4. **Create a Test File (`emp.txt`):**
    ```bash
    echo "This is text." > emp.txt
    ```

5. **Upload the File to Azure Blob Storage:**
    ```bash
    az storage blob upload --account-name ssacloudadfstorage --name input/emp.txt --container-name cont --file emp.txt --auth-mode key
    ```

## 2. Create a Data Factory

1. **Create Data Factory:**
    ```bash
    az datafactory create --resource-group mainRG --factory-name ssadcloudmainadf
    ```

2. **Verify Data Factory Creation:**
    ```bash
    az datafactory show --resource-group mainRG --factory-name ssadcloudmainadf
    ```

## 3. Create a Linked Service and Datasets

1. **Get Storage Account Connection String:**
    ```bash
    az storage account show-connection-string --resource-group mainRG --name ssacloudadfstorage --key primary
    ```

2. **Create Linked Service JSON File (`AzureStorageLinkedService.json`):**
    $ nano AzureStorageLinkedService.json 
      (Note: Exit -> CTRL + X -> Y)
    ```json
    {
        "type": "AzureBlobStorage",
        "typeProperties": {
            "connectionString": "DefaultEndpointsProtocol=https;AccountName=<accountName>;AccountKey=<accountKey>;EndpointSuffix=core.windows.net"
        }
    }
    ```
Sample:
{
  "connectionString": "DefaultEndpointsProtocol=https;
  AccountName=ssacloudadfstorage;
  AccountKey=tEJQ78Fimt4iPj+xNFlzQ3BpyIGNSu7j9KE8qOCdZHIAte0JghsiWnQ4NJPusxRXP5Ysom8igg8E+AStwJvbBQ==;
  EndpointSuffix=core.windows.net;
}
    

3. **Create Linked Service:**
    ```bash
    az datafactory linked-service create --resource-group mainRG --factory-name ssadcloudmainadf --linked-service-name AzureStorageLinkedService --properties AzureStorageLinkedService.json
    ```

4. **Create Input Dataset JSON File (`InputDataset.json`):**
    $ nano InputDataset.json
    ```json
    {
        "linkedServiceName": {
            "referenceName": "AzureStorageLinkedService",
            "type": "LinkedServiceReference"
        },
        "type": "Binary",
        "typeProperties": {
            "location": {
                "type": "AzureBlobStorageLocation",
                "fileName": "emp.txt",
                "folderPath": "input",
                "container": "cont"
            }
        }
    }
    ```

5. **Create Input Dataset:**
    ```bash
    az datafactory dataset create --resource-group mainRG --dataset-name InputDataset --factory-name ssadcloudmainadf --properties InputDataset.json
    ```

6. **Create Output Dataset JSON File (`OutputDataset.json`):**
    ```json
    {
        "linkedServiceName": {
            "referenceName": "AzureStorageLinkedService",
            "type": "LinkedServiceReference"
        },
        "type": "Binary",
        "typeProperties": {
            "location": {
                "type": "AzureBlobStorageLocation",
                "folderPath": "output",
                "container": "cont"
            }
        }
    }
    ```

7. **Create Output Dataset:**
    ```bash
    az datafactory dataset create --resource-group mainRG --dataset-name OutputDataset --factory-name ssadcloudmainadf --properties OutputDataset.json
    ```

## 4. Create and Run the Pipeline

1. **Create Pipeline JSON File (`CopyPipeline.json`):**
    ```json
    {
        "name": "CopyPipeline",
        "properties": {
            "activities": [
                {
                    "name": "CopyFromBlobToBlob",
                    "type": "Copy",
                    "typeProperties": {
                        "source": {
                            "type": "BinarySource",
                            "storeSettings": {
                                "type": "AzureBlobStorageReadSettings",
                                "recursive": true
                            }
                        },
                        "sink": {
                            "type": "BinarySink",
                            "storeSettings": {
                                "type": "AzureBlobStorageWriteSettings"
                            }
                        },
                        "enableStaging": false
                    },
                    "inputs": [
                        {
                            "referenceName": "InputDataset",
                            "type": "DatasetReference"
                        }
                    ],
                    "outputs": [
                        {
                            "referenceName": "OutputDataset",
                            "type": "DatasetReference"
                        }
                    ]
                }
            ]
        }
    }
    ```

2. **Create Pipeline:**
    ```bash
    az datafactory pipeline create --resource-group mainRG --factory-name ssadcloudmainadf --name CopyPipeline --pipeline CopyPipeline.json
    ```

3. **Run the Pipeline:**
    ```bash
    az datafactory pipeline create-run --resource-group mainRG --factory-name ssadcloudmainadf --name CopyPipeline
    ```

4. **Check Pipeline Run Status:**
    Replace `<run-id>` with the actual run ID returned from the previous step.
    ```bash
    az datafactory pipeline-run show --resource-group mainRG --factory-name ssadcloudmainadf --run-id <run-id>
    ```

## 5. Clean Up Resources

1. **Delete the Resource Group (and all associated resources):**
    ```bash
    az group delete --name mainRG --yes --no-wait
    ```

2. **To delete individual resources (e.g., linked service):**
    ```bash
    az datafactory linked-service delete --resource-group mainRG --factory-name ssadcloudmainadf --linked-service-name AzureStorageLinkedService
    ```

3. **Delete JSON Files Locally:**
    ```bash
    rm AzureStorageLinkedService.json InputDataset.json OutputDataset.json CopyPipeline.json
    ```