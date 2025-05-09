#!/bin/bash

# Directory to count files in (can be passed as argument or hardcoded)
TARGET_DIR="${1:-.}"  # default to current directory if no argument given

# Check if directory exists
if [ ! -d "$TARGET_DIR" ]; then
  echo "Directory not found: $TARGET_DIR"
  exit 1
fi

# Count number of regular files (not directories)
FILE_COUNT=$(find "$TARGET_DIR" -type f | wc -l)

echo "Number of files in '$TARGET_DIR': $FILE_COUNT"