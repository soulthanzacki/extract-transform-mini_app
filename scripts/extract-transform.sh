#!/bin/bash

YEAR=$1

if [ -z "$YEAR" ]; then
    echo "Error: Year is required as an argument."
    exit 1
fi

SCRIPT_DIR="/home/soulthanzacki/zacki/scripts"
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

echo "Starting data extraction and transformation for year $YEAR..."

/usr/bin/python3 "$SCRIPT_DIR/extract.py" "$YEAR" 
if [ $? -ne 0 ]; then
    echo "Failed to run extract.py"
    exit 1
fi

/usr/bin/python3 "$SCRIPT_DIR/transform.py" "$YEAR"
if [ $? -ne 0 ]; then
    echo "Failed to run transform.py"
    exit 1
fi

echo "Data extraction and transformation for $YEAR completed successfully."