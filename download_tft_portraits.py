import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
from config import ASSETS_BASE, CHAMPION_JSON_FILENAME, SET_PREFIX, PORTRAITS_DIR, CURRENT_SET

def load_champions_from_json():
    """Load champions from the TFT Set JSON file"""
    with open(CHAMPION_JSON_FILENAME, 'r') as f:
        champions_data = json.load(f)
    
    # Filter for current set champions and extract their character names
    champions = []
    for champ in champions_data:
        char_name = champ.get('characterName', '')
        if char_name.startswith(SET_PREFIX):
            champions.append(char_name.lower())
    
    return champions

def download_image(champion):
    """Download standard and evolved versions of champion portraits"""
    # Construct the URL path
    url_path = f"{champion}/skins/base/images/"
    
    # Try standard and evolved variations
    variations = [
        f"{champion}.tft_set{CURRENT_SET}.png",
        f"{champion}_mobile.tft_set{CURRENT_SET}.png",
        f"{champion}.tft_set{CURRENT_SET}_evolved.png",
        f"{champion}_mobile.tft_set{CURRENT_SET}_evolved.png"
    ]
    
    # Track if any downloads were successful
    successful_downloads = False
    temp_dir = os.path.join(PORTRAITS_DIR, "temp_" + champion)
    os.makedirs(temp_dir, exist_ok=True)
    
    for filename in variations:
        # URL encode the filename
        encoded_filename = quote(filename)
        full_url = f"{ASSETS_BASE}/characters/{url_path}{encoded_filename}"
        
        # Output path in temporary directory
        output_path = os.path.join(temp_dir, filename)
        
        try:
            response = requests.get(full_url)
            
            if response.status_code == 200:
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                print(f"Successfully downloaded {filename} for {champion}")
                successful_downloads = True
            else:
                print(f"File not found: {filename} for {champion}")
        except Exception as e:
            print(f"Error downloading {filename} for {champion}: {e}")
    
    # If any downloads were successful, move files to final location
    if successful_downloads:
        final_dir = os.path.join(PORTRAITS_DIR, champion)
        os.rename(temp_dir, final_dir)
    else:
        # Clean up temporary directory if no downloads were successful
        os.rmdir(temp_dir)

def main():
    print(f"Starting download of TFT Set {CURRENT_SET} champion portraits...")
    print(f"Images will be saved to: {os.path.abspath(PORTRAITS_DIR)}")
    
    # Load champions from JSON file
    champions = load_champions_from_json()
    print(f"Found {len(champions)} Set {CURRENT_SET} champions in JSON file")
    
    # Use ThreadPoolExecutor to download images in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_image, champions)
    
    print(f"Download complete! Check the {PORTRAITS_DIR} folder for the images.")

if __name__ == "__main__":
    main() 