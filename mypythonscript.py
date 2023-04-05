import os
import requests
import json


SUBSCRIPTION_ID = os.environ["subscription_id"]
TENANT_ID = os.environ["tenant_id"]
RESOURCE_GROUP_NAME = os.environ["rg-videos-name"]
SYNAPSE_WORKSPACE_NAME = os.environ["synapse_videos_workspace_name"]
CLIENT_ID = os.environ["sp-videos-client-id"]
CLIENT_SECRET = os.environ["sp-videos-client-secret"]

def main():
    # Acquire an access token
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    data = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
            "resource": "https://management.azure.com/"
        }
    token_request = requests.get(url=url, data=data)
    access_token = token_request.json()["access_token"]

    # Create the Azure Synapse Integration Runtime
    url = (
        f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/"
        f"resourceGroups/{RESOURCE_GROUP_NAME}/providers/Microsoft.Synapse/"
        f"workspaces/{SYNAPSE_WORKSPACE_NAME}/integrationRuntimes/mynewir1?api-version=2021-06-01-preview"
    )    
    headers = {
            "Authorization": f"Bearer {access_token}", 
            "Content-Type": "application/json"
            }
    data = {
        "properties": {
        "type": "Managed",
        "typeProperties": {
            "computeProperties": {
                "location": "AutoResolve",
                "dataFlowProperties": {
                    "computeType": "General",
                    "coreCount": 8,
                    "timeToLive": 0
                    }
                }
            }
        }
    }
    create_ir_request = requests.put(url=url, headers=headers, data=json.dumps(data))
    if create_ir_request.status_code != 200:
        create_ir_request.raise_for_status()
    print(create_ir_request.content)

if __name__ == '__main__':
    main()
