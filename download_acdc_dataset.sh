#!/bin/bash

###############################################################################
# ACDC Dataset Downloader
# This script downloads the full ACDC (Automated Cardiac Diagnosis Challenge)
# dataset from Kaggle using the Kaggle CLI.
###############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}ACDC Dataset Downloader${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if kaggle CLI is installed
if ! command -v kaggle &> /dev/null; then
    echo -e "${RED}Error: Kaggle CLI is not installed!${NC}"
    echo "Please install it with: pip install kaggle"
    exit 1
fi

# Check if kaggle.json exists in current directory
if [ ! -f "kaggle.json" ]; then
    echo -e "${RED}Error: kaggle.json not found in current directory!${NC}"
    echo "Please download it from https://www.kaggle.com/me/account (API section)"
    exit 1
fi

# Set up Kaggle credentials
echo -e "${YELLOW}Setting up Kaggle credentials...${NC}"
mkdir -p ~/.kaggle
cp kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
echo -e "${GREEN}✓ Credentials configured${NC}"

# Create data directory structure
echo -e "${YELLOW}Creating data directories...${NC}"
mkdir -p data/acdc_full
cd data/acdc_full

# The ACDC dataset on Kaggle has different variations
# Based on the notebook input page, the main datasets are:
DATASETS=(
    "anhoangvo/acdc-dataset"
    "trnaacthng/acdc-training-slices"
    "aysendegerli/acdcautomatedcardiacdiagnosischallenge"
    "kmader/acdc-database-automatic-cardiac-diagnosis"
)

echo -e "${GREEN}Available ACDC datasets to try:${NC}"
for i in "${!DATASETS[@]}"; do
    echo "  $((i+1)). ${DATASETS[$i]}"
done
echo ""
echo -e "${YELLOW}NOTE: You may need to:${NC}"
echo "  1. Go to https://www.kaggle.com/datasets/<dataset-slug>"
echo "  2. Click 'Download' or view the dataset page"
echo "  3. Accept any terms if prompted"
echo ""

# Try downloading the first dataset (most common one)
DATASET="${DATASETS[0]}"
echo ""
echo -e "${YELLOW}Downloading dataset: ${DATASET}${NC}"
echo "This may take several minutes depending on your connection..."

if kaggle datasets download -d "$DATASET" --unzip; then
    echo -e "${GREEN}✓ Successfully downloaded and extracted: ${DATASET}${NC}"
else
    echo -e "${YELLOW}First dataset failed, trying alternative...${NC}"
    DATASET="${DATASETS[1]}"
    echo -e "${YELLOW}Downloading dataset: ${DATASET}${NC}"
    
    if kaggle datasets download -d "$DATASET" --unzip; then
        echo -e "${GREEN}✓ Successfully downloaded and extracted: ${DATASET}${NC}"
    else
        echo -e "${YELLOW}Trying third alternative...${NC}"
        DATASET="${DATASETS[2]}"
        echo -e "${YELLOW}Downloading dataset: ${DATASET}${NC}"
        
        if kaggle datasets download -d "$DATASET" --unzip; then
            echo -e "${GREEN}✓ Successfully downloaded and extracted: ${DATASET}${NC}"
        else
            echo -e "${YELLOW}Trying fourth alternative...${NC}"
            DATASET="${DATASETS[3]}"
            echo -e "${YELLOW}Downloading dataset: ${DATASET}${NC}"
            
            if kaggle datasets download -d "$DATASET" --unzip; then
                echo -e "${GREEN}✓ Successfully downloaded and extracted: ${DATASET}${NC}"
            else
                echo -e "${RED}Failed to download all datasets. Please try manual approach:${NC}"
                echo ""
                echo "MANUAL DOWNLOAD STEPS:"
                echo "  1. Visit each dataset page on Kaggle:"
                for ds in "${DATASETS[@]}"; do
                    echo "     https://www.kaggle.com/datasets/$ds"
                done
                echo ""
                echo "  2. Click 'Download' button and accept any terms"
                echo "  3. Extract the ZIP file to: $(pwd)"
                echo ""
                echo "OR search for 'ACDC dataset' on Kaggle and download the one with most training slices"
                exit 1
            fi
        fi
    fi
fi

# Show what was downloaded
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Download complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Files downloaded to: $(pwd)"
echo ""
echo "Directory structure:"
ls -lh

# Count files
echo ""
echo "Statistics:"
echo "  Total files: $(find . -type f | wc -l)"
echo "  Total size: $(du -sh . | cut -f1)"

# Check for common ACDC structures
if [ -d "ACDC_training_slices" ] || [ -d "training" ] || [ -d "ACDC" ]; then
    echo -e "${GREEN}✓ ACDC dataset structure detected${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Update the ACDC loader in src/dataset/acdc_loader.py"
    echo "  2. Point it to the correct data path"
    echo "  3. Restart the dataset-generator-service"
else
    echo -e "${YELLOW}⚠ Unexpected directory structure. Please check the downloaded files.${NC}"
fi

cd ../..
echo ""
echo -e "${GREEN}Done!${NC}"
