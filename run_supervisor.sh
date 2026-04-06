#!/bin/bash

# Script to run Tiki Pipeline with Auto-Reset capabilities

echo "Starting Tiki Supervisor..."

# Kích hoạt môi trường ảo tự động để tránh lỗi thiếu thư viện
source env/bin/activate

while true; do
    python3 tiki.py
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