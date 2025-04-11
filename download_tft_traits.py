"""
Downloads TFT trait icons from Community Dragon
"""
import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
from config import ASSETS_BASE, TRAITS_JSON_FILENAME, TRAITS_DIR, CURRENT_SET

def get_traits():
    """Extract traits and their icon paths from the traits JSON file"""
    with open(TRAITS_JSON_FILENAME, 'r') as f:
        traits_data = json.load(f)
    
    # Extract trait names and icons
    trait_icons = []
    for trait in traits_data:
        icon_path = trait.get('icon', '')
        if icon_path:
            # Convert .tex extension to .png
            icon_path = icon_path.replace('.tex', '.png')
            # Remove 'ASSETS/' prefix as it's included in ASSETS_BASE
            icon_path = icon_path.replace('ASSETS/', '')
            trait_icons.append(icon_path)
    
    return trait_icons

def download_trait_icon(icon_path):
    """Download a trait icon"""
    # URL encode the filename
    encoded_path = quote(icon_path)
    full_url = f"{ASSETS_BASE}/{encoded_path}"
    
    # Output path - use just the filename part
    filename = os.path.basename(icon_path)
    output_path = os.path.join(TRAITS_DIR, filename)
    
    try:
        response = requests.get(full_url)
        
        if response.status_code == 200:
            with open(output_path, 'wb') as f:
                f.write(response.content)
            print(f"Successfully downloaded {filename}")
        else:
            print(f"Failed to download {filename} (Status: {response.status_code})")
            # Try alternate URL format for some traits
            alternate_path = f"ux/traiticons/{filename.lower()}"
            alternate_url = f"{ASSETS_BASE}/{alternate_path}"
            response = requests.get(alternate_url)
            if response.status_code == 200:
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                print(f"Successfully downloaded {filename} using alternate path")
            else:
                print(f"Failed to download {filename} using alternate path")
    except Exception as e:
        print(f"Error downloading {filename}: {e}")

def main():
    """Main entry point for trait icon downloads"""
    print(f"Starting download of TFT Set {CURRENT_SET} trait icons...")
    print(f"Icons will be saved to: {os.path.abspath(TRAITS_DIR)}")
    
    # Create traits directory if it doesn't exist
    os.makedirs(TRAITS_DIR, exist_ok=True)
    
    # Get list of trait icons
    trait_icons = get_traits()
    print(f"Found {len(trait_icons)} trait icons in Set {CURRENT_SET}")
    
    # Download trait icons in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_trait_icon, trait_icons)
    
    print(f"Download complete! Check the {TRAITS_DIR} folder for the images.")

if __name__ == "__main__":
    main() 