from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


credential = DefaultAzureCredential()

client = SecretClient(
    vault_url="https://dp-keyvault-all.vault.azure.net/",
    credential=credential
)


def get_secret(secretName):

    secret = client.get_secret(secretName)
    return secret.value
