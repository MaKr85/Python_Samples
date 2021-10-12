# pip install #
# azure-identity, azure-keyvault-secrets, azure-storage-file-datalake

import data_lake_helper
import form_recognizer_train
import form_recognizer_analyze
import json
import key_vault_helper

########### Parameter setzen #############
# DataLake
storageAccountName = "dpstoragetstmkr"
storageAccountKey = key_vault_helper.get_secret(storageAccountName) #"9fHjRnxj7oU9/cNYN8u/zK1kCDhiI3N/SVGJM4P5vqJah9LA+D9yrSS2CNYQKIrXX4g2pH9IKESOCeYLi1hc/w=="
serviceURI = "https://" + storageAccountName + ".dfs.core.windows.net/"

# Source
containerNameSource = "formrecognizer"
directoryNameSource = "EatHappy/deliverynote"
fileNameSource = "deliverynote_16529215.pdf"

# Form Recognizer
formRecognizerName = "dp-formrecognizer-mkr-tst"
endpoint = r"https://"+formRecognizerName+".cognitiveservices.azure.com/"
post_url = endpoint + "/formrecognizer/v2.1/custom/models"
source = r"https://dpstoragetstmkr.blob.core.windows.net/formrecognizer?sp=rl&st=2021-10-08T11:11:32Z&se=2021-12-31T20:11:32Z&spr=https&sv=2020-08-04&sr=c&sig=3CXAiIv1bzOrX8eZYKUTyO2Im4lwTDboaM%2Ff2eNrx8M%3D"
prefix = directoryNameSource
includeSubFolders = False
useLabelFile = False
apim_key = key_vault_helper.get_secret(formRecognizerName)  #"8ea662154e2b476bb45ee681a37bec6b"
fyle_type = "application/pdf"



########### train and get modelId #############
train_json = form_recognizer_train.trainModel(endpoint,post_url,source,prefix,includeSubFolders,useLabelFile,apim_key)
model_id = str(train_json['modelInfo']['modelId'])


########### analyze form #############
# Connect to DataLake
data_lake_helper.initialize_storage_account(storageAccountName,storageAccountKey)

# download file
data_bytes = data_lake_helper.download_file(containerNameSource,directoryNameSource,fileNameSource).readall()

# analyze and store json result
analyze_json = form_recognizer_analyze.analyzeForm(model_id,endpoint,apim_key,fyle_type,data_bytes)
data = json.dumps(analyze_json)
status = data_lake_helper.upload_file_sink(containerNameSource,directoryNameSource,fileNameSource+".json",data)
print(status)