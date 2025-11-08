#!/bin/bash
# Setup script for the Greenhouse Gas Emission Analysis project

echo "========================================"
echo "Setting up PySpark Data Engineering Project"
echo "========================================"

# Check Python version
echo -e "\nChecking Python version..."
python3 --version

if [ $? -ne 0 ]; then
    echo "❌ Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

# Check Java version
echo -e "\nChecking Java version..."
java -version

if [ $? -ne 0 ]; then
    echo "❌ Java is not installed. Please install Java 8 or higher for PySpark."
    exit 1
fi

# Install Python dependencies
echo -e "\nInstalling Python dependencies..."
pip install -r requirements.txt

if [ $? -ne 0 ]; then
    echo "❌ Failed to install dependencies."
    exit 1
fi

# Create output directories
echo -e "\nCreating output directories..."
mkdir -p output/reports
mkdir -p output/transformed_data
mkdir -p output/analysis
mkdir -p logs

echo -e "\n✓ Setup completed successfully!"
echo -e "\nYou can now run the application with:"
echo "  python main.py"
echo ""
