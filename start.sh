#!/bin/bash

# KPI Architecture Visualization - Start Script
# This script sets up a Python virtual environment, installs dependencies,
# and starts the visualization server on port 6060.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR="venv"
PORT="${PORT:-6060}"

echo "=============================================="
echo "  KPI Architecture Visualization Setup"
echo "=============================================="

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not installed."
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo ""
    echo "[1/3] Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    echo "      Virtual environment created at: $VENV_DIR/"
else
    echo ""
    echo "[1/3] Virtual environment already exists at: $VENV_DIR/"
fi

# Activate virtual environment
echo ""
echo "[2/3] Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Install dependencies
echo ""
echo "[3/3] Installing dependencies..."
pip install --upgrade pip -q
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt -q
    echo "      Dependencies installed from requirements.txt"
else
    echo "      No requirements.txt found (using standard library only)"
fi

# Start the server
echo ""
echo "=============================================="
echo "  Starting server on port $PORT"
echo "=============================================="
echo ""

export PORT="$PORT"
python3 kpi-visualization/serve.py
