#!/bin/bash

# Set up script variables
PYTHON_SCRIPT="data_pipeline.py"
VERSION_FILE="version.txt"
LOG_FILE="data_pipeline.log"
PROD_DIR="../prod"
EXCEL_FILE="students_data.csv"

# Execute Python script
echo "Running pipeline"
python3 $PYTHON_SCRIPT

# Check the latest version from the versions file
LATEST_VERSION=$(head -n 1 $VERSION_FILE)

# Check logs if any errors occured
if grep -A 100 "Version $LATEST_VERSION" "$LOG_FILE" | grep -q "ERROR"
then 
  echo "Pipeline failed in version $LATEST_VERSION. Check the logs for more details."
  # Exit the script with an error code
  exit 1 
else
  echo "Pipeline version $LATEST_VERSION completed successfully."
fi

# Move files from dev into prod
if [ ! -d "$PROD_DIR" ]; then
    mkdir -p "$PROD_DIR"
    echo "Created production directory: $PROD_DIR"
fi

mv "$EXCEL_FILE" "$PROD_DIR"
echo "File moved to $PROD_DIR"

echo "Finished the pipeline"