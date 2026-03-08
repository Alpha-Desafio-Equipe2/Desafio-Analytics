from pathlib import Path

"""
Path Configuration Module

This module defines the central directory paths used throughout the project, ensuring consistency when accessing data, raw files, and processed files.
"""
BASE_DIR = Path(__file__).resolve().parents[3]

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"