import requests
import json

url = "https://itviec.com/api/v1/tags/populars"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)


# Writing to sample.json
with open("sample.json", "w") as outfile:
    json.dump(response.json(), outfile)

print(response.text)
