import requests
import json

# --- Configuration ---
BASE_URL = "http://127.0.0.1:5057"  # Standard Flask development server URL


def fetch_dj_report(dj_name: str):
    """
    Calls the Flask API to get an analytics report for a given DJ.
    """
    # Construct the full API URL
    url = f"{BASE_URL}/api/dj/analytics/{dj_name}"
    print(f"▶️  Requesting report for '{dj_name}' from {url}")

    try:
        # Make the GET request
        response = requests.get(url)

        # Check the HTTP status code of the response
        if response.status_code == 200:
            print("✅ Success! Report received.")
            # Parse the JSON response
            report = response.json()
            # Print the formatted JSON report
            print(json.dumps(report, indent=4, default=str))
        else:
            # If the status code is not 200, print the error
            print(f"❌ Error: Received status code {response.status_code}")
            print("   Response:", response.json())

    except requests.exceptions.ConnectionError as e:
        print("❌ Connection Error: Could not connect to the API.")
        print("   Please ensure the Flask server (app.py) is running.")


if __name__ == '__main__':
    existing_dj = "DJ Marlone"
    fetch_dj_report(existing_dj)

    print("\n" + "=" * 50 + "\n")

    # --- Test Case 2: A DJ that should NOT exist ---
    non_existing_dj = "DJ Fake"
    fetch_dj_report(non_existing_dj)