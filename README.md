# TFT Set 13 Champion Portrait Downloader

This script downloads champion portrait images for Teamfight Tactics (TFT) Set 13 from CommunityDragon.

## Requirements

- Python 3.6 or higher
- `requests` library
- `Pillow` library (for verification script)

## Installation

1. Clone or download this repository
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Downloading Images

Simply run the download script:

```bash
python download_tft_portraits.py
```

The script will:
1. Create a directory called `tft13_portraits`
2. Download both regular and mobile portrait images for each TFT Set 13 champion
3. Save the images in champion-specific subdirectories

### Verifying Downloads

After downloading, you can verify the integrity of the downloaded images:

```bash
python verify_downloads.py
```

This will:
1. Check if all expected champion directories exist
2. Verify that all expected image files exist
3. Validate that each image is a proper PNG file
4. Provide a summary of valid and invalid/missing images

## Output Structure

```
tft13_portraits/
├── tft13_akali/
│   ├── tft13_akali.tft_set13.png
│   └── tft13_akali_mobile.tft_set13.png
├── tft13_ambessa/
│   ├── tft13_ambessa.tft_set13.png
│   └── tft13_ambessa_mobile.tft_set13.png
└── ...
```

## Notes

- The script uses ThreadPoolExecutor to download multiple images in parallel
- If an image fails to download, an error message will be displayed but the script will continue with other downloads
- All images are sourced from raw.communitydragon.org

## Credits

- All images are owned by Riot Games
- CommunityDragon was created under Riot Games' "Legal Jibber Jabber" policy using assets owned by Riot Games. Riot Games does not endorse or sponsor this project.

