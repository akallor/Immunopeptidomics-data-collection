#!/usr/bin/env python3
"""
PRIDE Archive Dataset Filter
Filters datasets for Immunopeptidomics + Cancer + timsTOF criteria
"""

import json
import csv
import re
from typing import List, Dict, Any

def detect_file_format(filename: str) -> str:
    """Detect the file format (JSON, CSV, TSV, etc.)."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            second_line = f.readline().strip()
            third_line = f.readline().strip()
        
        print(f"DEBUG - First line: {repr(first_line[:100])}")
        print(f"DEBUG - Second line: {repr(second_line[:100])}")
        print(f"DEBUG - Third line: {repr(third_line[:100])}")
        
        # Check if it starts with JSON markers
        if first_line.startswith('{') or first_line.startswith('['):
            return 'json'
        
        # Check for common CSV/TSV patterns
        if '\t' in first_line:
            return 'tsv'
        elif ',' in first_line and not first_line.startswith('{'):
            return 'csv'
        
        # Check for other patterns
        if first_line.startswith('"') and (',' in first_line or '\t' in first_line):
            return 'csv' if ',' in first_line else 'tsv'
        
        return 'unknown'
    except Exception as e:
        print(f"Error detecting format: {e}")
        return 'unknown'

def load_pride_data(filename: str) -> List[Dict[str, Any]]:
    """Load PRIDE datasets from various file formats."""
    file_format = detect_file_format(filename)
    print(f"Detected file format: {file_format}")
    
    if file_format == 'json':
        return load_pride_json(filename)
    elif file_format in ['csv', 'tsv']:
        return load_pride_csv(filename, file_format)
    else:
        print("Unknown file format. Showing first few lines:")
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= 10:  # Show first 10 lines
                        break
                    print(f"Line {i+1}: {repr(line.rstrip())}")
        except:
            pass
        return []

def load_pride_csv(filename: str, file_format: str) -> List[Dict[str, Any]]:
    """Load PRIDE datasets from CSV/TSV file."""
    datasets = []
    delimiter = '\t' if file_format == 'tsv' else ','
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            
            print(f"CSV/TSV columns found: {reader.fieldnames}")
            
            for row in reader:
                # Convert CSV row to our expected format
                dataset = {}
                
                # Map common column names to our expected keys
                column_mapping = {
                    'accession': ['accession', 'id', 'dataset_id', 'pride_id'],
                    'title': ['title', 'dataset_title', 'name'],
                    'projectDescription': ['description', 'project_description', 'summary'],
                    'keywords': ['keywords', 'tags'],
                    'instruments': ['instruments', 'instrument', 'ms_instrument'],
                    'submissionDate': ['submission_date', 'submitted', 'date_submitted'],
                    'publicationDate': ['publication_date', 'published', 'date_published'],
                    'doi': ['doi', 'publication_doi'],
                    'submitters': ['submitters', 'authors', 'contact']
                }
                
                # Map columns to standard format
                for standard_key, possible_columns in column_mapping.items():
                    for col in possible_columns:
                        if col in row and row[col]:
                            dataset[standard_key] = row[col]
                            break
                
                # Add any unmapped columns as-is
                for key, value in row.items():
                    if key not in [col for cols in column_mapping.values() for col in cols]:
                        dataset[key] = value
                
                datasets.append(dataset)
            
            print(f"Loaded {len(datasets)} datasets from {file_format.upper()} file.")
            return datasets
    
    except Exception as e:
        print(f"Error loading {file_format.upper()} file: {e}")
        return []

def load_pride_json(filename: str) -> List[Dict[str, Any]]:
    """Load the PRIDE datasets JSON file, handling multiple formats."""
    datasets = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        # First, try to load as standard JSON
        try:
            data = json.loads(content)
            print(f"Successfully loaded JSON. Type: {type(data)}")
            
            # Handle different JSON structures
            if isinstance(data, list):
                print(f"Found JSON array with {len(data)} items")
                return data
            elif isinstance(data, dict):
                print(f"Found JSON object with keys: {list(data.keys())}")
                
                # Check for common dataset container keys
                if 'datasets' in data:
                    print(f"Found 'datasets' key with {len(data['datasets'])} items")
                    return data['datasets']
                elif 'projects' in data:
                    print(f"Found 'projects' key with {len(data['projects'])} items")
                    return data['projects']
                elif 'data' in data:
                    print(f"Found 'data' key with {len(data['data'])} items")
                    return data['data']
                else:
                    # If it's a single dataset object, wrap in list
                    print("Treating as single dataset object")
                    return [data]
            else:
                print(f"Unexpected JSON type: {type(data)}")
                return []
        
        except json.JSONDecodeError as e:
            print(f"JSON parsing failed: {e}")
            
            # Check if this is concatenated JSON objects (multiple objects separated by }\n{)
            if "Extra data:" in str(e):
                print("Detected concatenated JSON objects. Attempting to split and parse...")
                
                # Split on }\n{ pattern but preserve the braces
                json_objects = []
                current_obj = ""
                brace_count = 0
                in_string = False
                escape_next = False
                
                for char in content:
                    current_obj += char
                    
                    if escape_next:
                        escape_next = False
                        continue
                    
                    if char == '\\':
                        escape_next = True
                        continue
                    
                    if char == '"' and not escape_next:
                        in_string = not in_string
                        continue
                    
                    if not in_string:
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            
                            if brace_count == 0:
                                # We've found a complete JSON object
                                json_objects.append(current_obj.strip())
                                current_obj = ""
                
                print(f"Found {len(json_objects)} JSON objects")
                
                # Parse each JSON object
                for i, obj_str in enumerate(json_objects):
                    if obj_str:
                        try:
                            dataset = json.loads(obj_str)
                            datasets.append(dataset)
                        except json.JSONDecodeError as obj_error:
                            print(f"Warning: Failed to parse JSON object {i+1}: {obj_error}")
                            continue
                
                if datasets:
                    print(f"Successfully loaded {len(datasets)} datasets from concatenated JSON format.")
                    return datasets
            
            # Try to fix common JSON issues as fallback
            print("Attempting to fix common JSON formatting issues...")
            
            # Remove trailing commas before closing braces/brackets
            fixed_content = re.sub(r',(\s*[}\]])', r'\1', content)
            
            # Try parsing the fixed content
            try:
                data = json.loads(fixed_content)
                print("Successfully fixed and loaded JSON!")
                
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    if 'datasets' in data:
                        return data['datasets']
                    elif 'projects' in data:
                        return data['projects']
                    else:
                        return [data]
            except json.JSONDecodeError as e2:
                print(f"Fixed JSON parsing also failed: {e2}")
                return []
    
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return []
    except Exception as e:
        print(f"Unexpected error reading file '{filename}': {e}")
        return []
    
    return []

def check_immunopeptidomics(dataset: Dict[str, Any]) -> bool:
    """Check if dataset is related to immunopeptidomics."""
    search_fields = [
        'title', 'projectDescription', 'keywords', 'projectTags'
    ]
    
    immunopep_terms = [
        'immunopeptidomics', 'immunopeptidome', 'immunopeptides',
        'mhc', 'hla', 'antigen presentation', 'peptide presentation',
        'major histocompatibility', 'immunopeptide'
    ]
    
    for field in search_fields:
        if field in dataset and dataset[field]:
            text = str(dataset[field]).lower()
            for term in immunopep_terms:
                if term in text:
                    return True
    return False

def check_cancer(dataset: Dict[str, Any]) -> bool:
    """Check if dataset is related to cancer."""
    search_fields = [
        'title', 'projectDescription', 'keywords', 'projectTags'
    ]
    
    cancer_terms = [
        'cancer', 'tumor', 'tumour', 'carcinoma', 'melanoma',
        'leukemia', 'lymphoma', 'oncology', 'neoplasm', 'malignant',
        'metastasis', 'adenocarcinoma', 'sarcoma', 'glioma'
    ]
    
    for field in search_fields:
        if field in dataset and dataset[field]:
            text = str(dataset[field]).lower()
            for term in cancer_terms:
                if term in text:
                    return True
    return False

def check_timstof(dataset: Dict[str, Any]) -> bool:
    """Check if dataset used timsTOF instruments."""
    # Check in instruments field
    if 'instruments' in dataset and dataset['instruments']:
        for instrument in dataset['instruments']:
            if isinstance(instrument, dict):
                # Check various possible fields in instrument object
                instrument_text = str(instrument).lower()
                if 'timstof' in instrument_text or 'tims-tof' in instrument_text:
                    return True
            elif isinstance(instrument, str):
                if 'timstof' in instrument.lower() or 'tims-tof' in instrument.lower():
                    return True
    
    # Also check in title and description as fallback
    search_fields = ['title', 'projectDescription']
    for field in search_fields:
        if field in dataset and dataset[field]:
            text = str(dataset[field]).lower()
            if 'timstof' in text or 'tims-tof' in text:
                return True
    
    return False

def extract_dataset_info(dataset: Dict[str, Any]) -> Dict[str, str]:
    """Extract relevant information from a dataset."""
    return {
        'accession': dataset.get('accession', ''),
        'title': dataset.get('title', ''),
        'description': dataset.get('projectDescription', ''),
        'keywords': str(dataset.get('keywords', '')),
        'submission_date': dataset.get('submissionDate', ''),
        'publication_date': dataset.get('publicationDate', ''),
        'doi': dataset.get('doi', ''),
        'instruments': str(dataset.get('instruments', '')),
        'submitters': str([f"{s.get('firstName', '')} {s.get('lastName', '')}" 
                          for s in dataset.get('submitters', []) if isinstance(s, dict)])
    }

def main():
    """Main function to process PRIDE datasets."""
    input_file = 'pride_datasets.json'
    output_file = 'pride_ip_datasets.tsv'
    
    print(f"Loading datasets from {input_file}...")
    datasets = load_pride_data(input_file)
    
    if not datasets:
        print("No datasets found or error loading file.")
        return
    
    print(f"Loaded {len(datasets)} datasets.")
    print("Filtering for Immunopeptidomics + Cancer + timsTOF criteria...")
    
    matching_datasets = []
    
    for dataset in datasets:
        is_immunopep = check_immunopeptidomics(dataset)
        is_cancer = check_cancer(dataset)
        is_timstof = check_timstof(dataset)
        
        if is_immunopep and is_cancer and is_timstof:
            matching_datasets.append(extract_dataset_info(dataset))
            print(f"✓ Found match: {dataset.get('accession', 'Unknown')} - {dataset.get('title', 'No title')[:80]}...")
    
    print(f"\nFound {len(matching_datasets)} datasets matching all criteria.")
    
    if matching_datasets:
        # Write to TSV file
        print(f"Saving results to {output_file}...")
        
        fieldnames = [
            'accession', 'title', 'description', 'keywords', 
            'submission_date', 'publication_date', 'doi', 
            'instruments', 'submitters'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
            writer.writeheader()
            writer.writerows(matching_datasets)
        
        print(f"✓ Results saved to {output_file}")
        
        # Print summary
        print(f"\nSummary:")
        print(f"- Total datasets processed: {len(datasets)}")
        print(f"- Matching datasets found: {len(matching_datasets)}")
        print(f"- Output saved to: {output_file}")
        
    else:
        print("No datasets found matching all criteria.")
        print("\nTip: You might want to check if:")
        print("- The search terms are too restrictive")
        print("- The JSON structure is different than expected")
        print("- The dataset contains the required information in different fields")

if __name__ == "__main__":
    main()
