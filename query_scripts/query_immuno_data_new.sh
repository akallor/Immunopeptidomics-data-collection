#!/bin/bash

# Script to combine multiple ProteomeXchange searches and deduplicate results
BASE_URL="https://proteomecentral.proteomexchange.org/api/proxi/v0.1/datasets"
OUTPUT_DIR="proteome_combined_results"
mkdir -p "$OUTPUT_DIR"

echo "=== ProteomeXchange Combined Search Strategy ==="
echo "Searching for all case variations and related terms..."
echo ""

# Function to perform paginated search
search_all_pages() {
    local keywords="$1"
    local output_file="$2"
    local page=1
    local page_size=200
    local all_datasets="[]"
    
    echo "Searching for: $keywords"
    
    while true; do
        echo "  Fetching page $page..."
        
        url="${BASE_URL}?pageSize=${page_size}&pageNumber=${page}&resultType=full&species=Homo%20sapiens&keywords=${keywords}"
        
        response=$(curl -s -X GET --header 'Accept: application/json' "$url")
        
        # Extract datasets from this page
        if command -v jq &> /dev/null; then
            page_datasets=$(echo "$response" | jq '.datasets' 2>/dev/null || echo "[]")
            dataset_count=$(echo "$page_datasets" | jq 'length' 2>/dev/null || echo "0")
        else
            echo "Error: jq is required for this script"
            exit 1
        fi
        
        if [ "$dataset_count" = "0" ]; then
            echo "  No more results found"
            break
        fi
        
        # Combine with previous results
        all_datasets=$(echo "$all_datasets $page_datasets" | jq -s 'add')
        
        echo "  Found $dataset_count results on page $page"
        
        # If we got less than page_size results, we're done
        if [ "$dataset_count" -lt "$page_size" ]; then
            echo "  Last page reached"
            break
        fi
        
        page=$((page + 1))
    done
    
    # Save results
    echo "$all_datasets" | jq '{datasets: .}' > "$output_file"
    total_count=$(echo "$all_datasets" | jq 'length')
    echo "  Total results: $total_count datasets saved to $output_file"
    echo ""
    
    return $total_count
}

# Search for different variations and related terms
search_terms=(
    "immunopeptidomics"
    "Immunopeptidomics" 
    "immunopeptidome"
    "Immunopeptidome"
    "cancer"
    "Cancer"
    "tumor"
    "Tumor"
    "tumour"
    "Tumour"
    "oncology"
    "Oncology"
    "malignant"
    "Malignant"
    "neoplasm"
    "Neoplasm"
    "MHC"
    "mhc"
    "HLA"
    "hla"
    "antigen%20presentation"
    "Antigen%20presentation"
)

# Perform all searches
for term in "${search_terms[@]}"; do
    safe_filename=$(echo "$term" | sed 's/%20/_/g' | tr '[:upper:]' '[:lower:]')
    search_all_pages "$term" "${OUTPUT_DIR}/search_${safe_filename}.json"
done

echo "=== Combining and Deduplicating Results ==="

# More robust approach to handle jq issues on Windows/Cygwin
echo "Merging all search results..."

# First, create a temporary file with all datasets
temp_file="${OUTPUT_DIR}/temp_all_datasets.json"
echo "[]" > "$temp_file"

# Process files one by one to avoid jq memory issues
for file in "${OUTPUT_DIR}"/search_*.json; do
    if [ -f "$file" ] && [ -s "$file" ]; then
        echo "Processing $(basename "$file")..."
        # Combine this file's datasets with the accumulated results
        jq -s '.[0] + .[1].datasets' "$temp_file" "$file" > "${temp_file}.tmp" && mv "${temp_file}.tmp" "$temp_file"
    fi
done

# Now deduplicate and create final structure
echo "Deduplicating results..."
jq '
    unique_by(.accession) | 
    {
        datasets: .,
        total_unique_datasets: length,
        search_timestamp: (now | strftime("%Y-%m-%d %H:%M:%S"))
    }
' "$temp_file" > "${OUTPUT_DIR}/combined_unique_results.json"

# Clean up temp file
rm "$temp_file"

total_unique=$(jq '.total_unique_datasets' "${OUTPUT_DIR}/combined_unique_results.json")
echo "Total unique datasets found: $total_unique"

# Create a detailed summary
echo "=== Creating Detailed Summary ==="
summary_file="${OUTPUT_DIR}/search_summary.txt"
{
    echo "ProteomeXchange Combined Search Summary"
    echo "======================================"
    echo "Search Date: $(date)"
    echo "Total Unique Datasets: $total_unique"
    echo ""
    echo "Individual Search Results:"
    echo "--------------------------"
    
    for file in "${OUTPUT_DIR}"/search_*.json; do
        if [ -f "$file" ]; then
            basename_file=$(basename "$file" .json | sed 's/search_//')
            count=$(jq '.datasets | length' "$file")
            echo "$basename_file: $count datasets"
        fi
    done
    
    echo ""
    echo "Files Created:"
    echo "--------------"
    echo "- combined_unique_results.json: All unique datasets combined"
    echo "- search_*.json: Individual search results"
    echo "- search_summary.txt: This summary"
    
} > "$summary_file"

echo "Summary saved to: $summary_file"
echo ""
echo "=== Final Results ==="
echo "Combined unique datasets: $total_unique"
echo "Main result file: ${OUTPUT_DIR}/combined_unique_results.json"
echo ""
echo "To view a sample of the results:"
echo "jq '.datasets[0:3] | .[] | {accession, title}' ${OUTPUT_DIR}/combined_unique_results.json"
