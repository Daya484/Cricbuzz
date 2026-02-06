import requests
import json

url = "https://cricbuzz-cricket2.p.rapidapi.com/teams/v1/international"

headers = {
	"x-rapidapi-key": "90afb8345bmsh4d4300b9e2bd9ddp114c1ajsnda1a354a12a4",
	"x-rapidapi-host": "cricbuzz-cricket2.p.rapidapi.com"
}
params = {
    'formatType': 'TeamInternational'
}

try:
    response = requests.get(url, headers=headers, params=params)
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print("Keys in response:", data.keys())
        print("Full response (indented):")
        print(json.dumps(data, indent=2)[:500]) # First 500 chars
    else:
        print("Response text:", response.text)
except Exception as e:
    print(f"Error: {e}")
