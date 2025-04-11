"""
Configuration settings for TFT asset downloader
"""

# Current TFT set number
CURRENT_SET = 14

# Derived variables
SET_PREFIX = f"TFT{CURRENT_SET}_"
CHAMPION_JSON_FILENAME = f"text_files/tft{CURRENT_SET}_champions.json"
TRAITS_JSON_FILENAME = f"text_files/tft{CURRENT_SET}_traits.json"

# Community Dragon base URLs
COMMUNITY_DRAGON_BASE = "https://raw.communitydragon.org/latest/game"
ASSETS_BASE = f"{COMMUNITY_DRAGON_BASE}/assets"
DATA_BASE = f"{COMMUNITY_DRAGON_BASE}/data"

# Output directories
PORTRAITS_DIR = f"tft{CURRENT_SET}_portraits"
TRAITS_DIR = f"tft{CURRENT_SET}_traits"
TEXT_FILES_DIR = "text_files"

# Ensure all required directories exist
REQUIRED_DIRS = [PORTRAITS_DIR, TRAITS_DIR, TEXT_FILES_DIR] 