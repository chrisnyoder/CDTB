import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
from time import sleep
import os.path
from config import CURRENT_SET, TEXT_FILES_DIR

BASE_URL = "https://raw.communitydragon.org/latest/cdragon/tft/en_us.json"

def create_trait_asset_map(traits_data):
    """
    Create a mapping of trait names to their asset filenames.
    
    Args:
        traits_data (list): List of trait dictionaries
    Returns:
        dict: Mapping of trait names to asset filenames
    """
    trait_map = {}
    for trait in traits_data:
        name = trait.get('name')
        icon = trait.get('icon')
        if name and icon:
            # Extract just the filename without path and extension
            filename = os.path.basename(icon)
            base_name = os.path.splitext(filename)[0]
            # Convert to lowercase and add .png extension
            asset_name = f"{base_name.lower()}.png"
            trait_map[name] = asset_name
    
    return trait_map

def download_and_process_json(url, output_dir):
    """
    Download a JSON file, process it, and split into separate files for the current TFT Set data.

    Args:
        url (str): The URL of the JSON file to download.
        output_dir (str): The directory to save the processed files.
    """
    try:
        print(f"Downloading data from {url}...")
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse the JSON data
        data = response.json()
        
        # Find current Set data
        set_data = None
        for set_info in data.get('setData', []):
            if set_info.get('number') == CURRENT_SET and set_info.get('mutator') == f'TFTSet{CURRENT_SET}':
                set_data = set_info
                break
        
        if not set_data:
            raise ValueError(f"Could not find TFT Set {CURRENT_SET} data in the response")
        
        # Create the individual JSON files
        file_mappings = {
            f'tft{CURRENT_SET}_augments.json': set_data.get('augments', []),
            f'tft{CURRENT_SET}_champions.json': set_data.get('champions', []),
            f'tft{CURRENT_SET}_items.json': set_data.get('items', []),
            f'tft{CURRENT_SET}_traits.json': set_data.get('traits', [])
        }
        
        # Save each file
        for filename, content in file_mappings.items():
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(content, f, indent=2, ensure_ascii=False)
            print(f"Created {filename}")
        
        # Create and save trait asset map
        traits_data = set_data.get('traits', [])
        trait_asset_map = create_trait_asset_map(traits_data)
        
        # Save trait asset map
        map_filepath = os.path.join(output_dir, 'trait_asset_map.json')
        with open(map_filepath, 'w', encoding='utf-8') as f:
            json.dump(trait_asset_map, f, indent=2, ensure_ascii=False)
        print("Created trait_asset_map.json")
            
        print("All files have been created successfully!")
        
    except Exception as e:
        print(f"Error processing data: {e}")
        raise

def main():
    print(f"Starting TFT Set {CURRENT_SET} data processing...")
    os.makedirs(TEXT_FILES_DIR, exist_ok=True)

    # Download and process the JSON file    
    download_and_process_json(BASE_URL, TEXT_FILES_DIR)

    print("Processing complete!")

if __name__ == "__main__":
    main()







