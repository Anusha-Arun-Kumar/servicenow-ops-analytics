
import os
import requests
from dotenv import load_dotenv

#Load environment variables from .env file
load_dotenv()

instance = os.getenv("SERVICENOW_INSTANCE")
user = os.getenv("SERVICENOW_USER")
password = os.getenv("SERVICENOW_PASSWORD")

#Quickly check if the environment variables are loaded correctly
if not instance or not user or not password:
    raise ValueError("Missing one or more ServiceNow env vars — check your .env file")

url = f"{instance}/api/now/table/incident"
params = {"sysparm_limit": 10}

response = requests.get(
    url,
    auth=(user, password),
    headers={"Accept": "application/json"},
    params=params
)

response.raise_for_status()  # throws an error if the request failed
data = response.json()

print(f"Pulled {len(data['result'])} incidents")
print(data['result'][0])  # peek at the first record's structure