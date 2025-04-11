#!/bin/bash

echo "TFT Set 13 Champion Portrait Downloader"
echo "======================================"
echo

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed or not in PATH."
    echo "Please install Python 3.6 or higher from https://www.python.org/downloads/"
    exit 1
fi

# Check if requirements are installed
echo "Checking and installing requirements..."
python3 -m pip install -r requirements.txt
if [ $? -ne 0 ]; then
    echo "Error: Failed to install requirements."
    exit 1
fi

echo
echo "Starting download of TFT Set 13 champion portraits..."
python3 download_tft_portraits.py
if [ $? -ne 0 ]; then
    echo "Error: Download script failed."
    exit 1
fi

echo
echo "Verifying downloaded images..."
python3 verify_downloads.py
if [ $? -ne 0 ]; then
    echo "Error: Verification script failed."
    exit 1
fi

echo
echo "Process completed!"
echo "Check the tft13_portraits directory for the downloaded images."
echo 