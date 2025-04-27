import requests
import json

BASE_URL = "https://api-hc.metatft.com/tft-comps-api/comps_data?queue=1100&patch=current&days=2&rank=CHALLENGER,DIAMOND,GRANDMASTER,MASTER&permit_filter_adjustment=true"
OUTPUT_FILE = "text_files/meta_comps_extracted.json"

def extract_comps(data):
    comps = {}
    cluster_details = data["results"]["data"]["cluster_details"]
    for cluster_id, comp in cluster_details.items():
        units_string = comp.get("units_string", "")
        levelling = comp.get("levelling", "")
        builds = comp.get("builds", [])
        builds_simple = [
            {"unit": b.get("unit", ""), "buildName": b.get("buildName", [])}
            for b in builds
        ]
        comps[cluster_id] = {
            "units_string": units_string,
            "builds": builds_simple,
            "levelling": levelling
        }
    return comps

def main():
    print(f"Downloading comps data from {BASE_URL} ...")
    response = requests.get(BASE_URL)
    response.raise_for_status()
    data = response.json()
    comps = extract_comps(data)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(comps, f, indent=2, ensure_ascii=False)
    print(f"Extracted comps saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main() 