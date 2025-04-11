"""
Main entry point for TFT asset downloader
"""
import os
import sys
from config import REQUIRED_DIRS

def setup_directories():
    """Create all required directories if they don't exist"""
    for directory in REQUIRED_DIRS:
        os.makedirs(directory, exist_ok=True)

def main():
    """Main entry point for the TFT asset downloader"""
    print("Starting TFT asset download process...")
    
    # Create required directories
    setup_directories()
    
    # Import and run each downloader in sequence
    try:
        print("\n1. Downloading text files...")
        from download_text_file import main as download_text
        download_text()
        
        print("\n2. Downloading champion portraits...")
        from download_tft_portraits import main as download_portraits
        download_portraits()
        
        print("\n3. Downloading trait icons...")
        from download_tft_traits import main as download_traits
        download_traits()
        
        print("\nAll downloads completed successfully!")
        
    except Exception as e:
        print(f"\nError during download process: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 