import requests
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from io import StringIO
import os

METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"

# Pull the csv download url from the distribution list
# Using mediaType match instead of just grabbing index 0 in case CMS adds other formats later
def get_csv_url(distribution):
    for dist in distribution:
        if dist.get("mediaType") == "text/csv":
            return dist.get("downloadURL")
    return None

# Strip special chars, collapse multiple spaces into single underscores, lowercase everything
def convert_to_snake_case(string):
    clean = re.sub(r'[^a-zA-Z0-9 ]', '', string)
    clean_underscores = re.sub(r' +', '_', clean)
    return clean_underscores.lower()

# Download a single csv, clean up the column names, and save it
def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()
    
    # low_memory=False so pandas doesn't complain about mixed types in footnote columns
    df = pd.read_csv(StringIO(response.text), low_memory=False)
    df.columns = [convert_to_snake_case(col) for col in df.columns]
    
    filename = url.split("/")[-1]
    df.to_csv(f"downloads/{filename}", index=False)
    return url

def fetch_metadata():
    response = requests.get(METASTORE_URL)
    response.raise_for_status()
    return response.json()

def main():
    datasets = fetch_metadata()
    download_list = []

    # Load tracking file from last run, or start fresh if it doesn't exist yet
    try:
        with open("tracking.json", "r") as f:
            tracking = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        tracking = {}
   
    # Filter to Hospital datasets and check if they're new or have been updated since last run
    for data in datasets:
        if "Hospitals" in data["theme"]:
            downloadURL = get_csv_url(data["distribution"])
            if data["identifier"] not in tracking:
                download_list.append({
                    "url": downloadURL,
                    "identifier": data["identifier"],
                    "modified": data["modified"]
                })
            elif data["modified"] > tracking[data["identifier"]]["modified"]:
                download_list.append({
                    "url": downloadURL,
                    "identifier": data["identifier"],
                    "modified": data["modified"]
                })
            else:
                pass

    if len(download_list) == 0:
        print("No new files or modified files.")
    else:
        print(f"Found {len(download_list)} out of date files. Downloading...")

    successful_downloads = 0

    # Download in parallel with 5 workers - don't want to slam CMS's servers
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(download_csv, item["url"]): item for item in download_list}
        for future in as_completed(futures):
            item = futures[future]
            try:
                result = future.result()
                new_file = item["url"].split("/")[-1]
                
                print(f"Downloaded: {new_file}")
                
                # If the file name changed (url hash is different), clean up the old one
                # Only delete after successful download so we don't lose data on failure
                if item["identifier"] in tracking:
                    old_file = tracking[item["identifier"]].get("file_name")
                    if old_file and old_file != new_file and os.path.exists(f"downloads/{old_file}"):
                        os.remove(f"downloads/{old_file}")

                tracking[item["identifier"]] = {
                    "modified": item["modified"],
                    "file_name": new_file
                }
                successful_downloads += 1
            except Exception as e:
                print(f"Failed: {item['url']} - {e}")

    if len(download_list) > 0:
        print(f"Successfully downloaded {successful_downloads}/{len(download_list)} files")

    # Save tracking state for next run
    with open("tracking.json", "w") as f:
        json.dump(tracking, f, indent=2)

if __name__ == "__main__":
    main()