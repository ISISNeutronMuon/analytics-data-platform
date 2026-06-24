#!/bin/bash

# Define the list of schemas to process in "SCHEMA_NAME:INCLUDE_VIEWS" format
# Set the second value to 'true' if you want to clone views for that schema
SCHEMAS=(
    "FACILITY_COMMON:false"
    "FACILITY_ERAS:false"
    "FACILITY_SAFETY_TEST:false"
    "FACILITY_SCHEDULE:false"
    "FACILITY_VISITS:false"
    "ISISUSERDB:false"
    "PROPOSAL_REGISTRY:false"
    "IOPS_V4:true"
    "PROP_CLF_ALL:true"
    "PROP_STATUS:true"
    "XPRESS_V4:true"
)

echo "Starting Oracle table cloning process..."
echo "---------------------------------------"

# Loop through each entry
for ENTRY in "${SCHEMAS[@]}"
do
    # Split the entry into schema name and the include_views flag
    SCHEMA="${ENTRY%%:*}"
    INCLUDE_VIEWS="${ENTRY#*:}"
    
    # Determine if we should add the flag to the command
    VIEW_FLAG=""
    if [ "$INCLUDE_VIEWS" = "true" ]; then
        VIEW_FLAG="--include-views"
    fi

    echo "Running: python3 ./clone_oracle_table.py $SCHEMA all $VIEW_FLAG"
    
    # Execute the command
    python3 ./clone_oracle_table.py "$SCHEMA" all $VIEW_FLAG
    
    # Check if the previous command succeeded
    if [ $? -eq 0 ]; then
        echo "✅ COMPLETED: $SCHEMA (Include Views: $INCLUDE_VIEWS)"
    else
        echo "❌ FAILED: $SCHEMA"
    fi
    
    echo "---------------------------------------"
done

echo "All tasks finished."