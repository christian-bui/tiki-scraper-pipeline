#!/bin/bash

# Script to run Tiki Pipeline with Auto-Reset capabilities

echo "Starting Tiki Supervisor..."

# Activate virtual environment
source env/bin/activate

while true; do
    # Chạy vô lăng chính ở thư mục gốc
    python3 main.py
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Pipeline finished successfully with exit code 0."
        break
    else
        echo "CRASH DETECTED: Pipeline exited with code $EXIT_CODE."
        echo "Auto-resetting in 5 seconds... Press Ctrl+C to abort."
        sleep 5
    fi
done