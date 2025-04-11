@echo off
echo TFT Set 13 Champion Portrait Downloader
echo ======================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Python is not installed or not in PATH.
    echo Please install Python 3.6 or higher from https://www.python.org/downloads/
    pause
    exit /b 1
)

REM Check if requirements are installed
echo Checking and installing requirements...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo Error: Failed to install requirements.
    pause
    exit /b 1
)

echo.
echo Starting download of TFT Set 13 champion portraits...
python download_tft_portraits.py
if %errorlevel% neq 0 (
    echo Error: Download script failed.
    pause
    exit /b 1
)

echo.
echo Verifying downloaded images...
python verify_downloads.py
if %errorlevel% neq 0 (
    echo Error: Verification script failed.
    pause
    exit /b 1
)

echo.
echo Process completed!
echo Check the tft13_portraits directory for the downloaded images.
echo.
pause 