import os
import sys
from PIL import Image

def verify_images(directory):
    """Verify that all downloaded images are valid PNG files"""
    print(f"Verifying images in {directory}...")
    
    total_images = 0
    valid_images = 0
    invalid_images = 0
    missing_champions = []
    
    # Get the list of expected champions from the download script
    try:
        sys.path.append('.')
        from download_tft_portraits import champions
    except ImportError:
        print("Error: Could not import champions list from download_tft_portraits.py")
        return
    
    # Check if the main directory exists
    if not os.path.exists(directory):
        print(f"Error: Directory {directory} does not exist!")
        return
    
    # Check each champion directory
    for champion in champions:
        champ_dir = os.path.join(directory, champion)
        
        if not os.path.exists(champ_dir):
            print(f"Warning: Directory for {champion} does not exist!")
            missing_champions.append(champion)
            continue
        
        # Expected filenames
        filenames = [
            f"{champion}.tft_set13.png",
            f"{champion}_mobile.tft_set13.png"
        ]
        
        for filename in filenames:
            file_path = os.path.join(champ_dir, filename)
            total_images += 1
            
            if not os.path.exists(file_path):
                print(f"Warning: Image {file_path} does not exist!")
                invalid_images += 1
                continue
            
            # Try to open the image to verify it's a valid PNG
            try:
                with Image.open(file_path) as img:
                    img.verify()
                valid_images += 1
            except Exception as e:
                print(f"Error: Image {file_path} is not a valid PNG file: {e}")
                invalid_images += 1
    
    # Print summary
    print("\nVerification Summary:")
    print(f"Total expected images: {total_images}")
    print(f"Valid images: {valid_images}")
    print(f"Invalid/missing images: {invalid_images}")
    
    if missing_champions:
        print(f"\nMissing champion directories ({len(missing_champions)}):")
        for champion in missing_champions:
            print(f"  - {champion}")

if __name__ == "__main__":
    verify_images("tft13_portraits") 