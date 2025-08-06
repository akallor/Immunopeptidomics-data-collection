#!/usr/bin/env python3
"""
Streaming JSON Parser for Very Large PRIDE Datasets

This script uses a streaming approach to parse large JSON files
that may have formatting issues with multi-line text fields.
"""

import json
import re
import csv
import sys
from typing import List, Dict, Any, Optional, Iterator

class StreamingJSONParser:
    """Class to handle streaming parsing of very large JSON files."""
    
    def __init__(self):
        # Define the fields we want to extract
        self.target_fields = {
            'accession': ['accession', 'id', 'dataset_id', 'pride_id'],
            'title': ['title', 'dataset_title', 'name'],
            'projectDescription': ['description', 'project_description', 'summary', 'projectDescription'],
            'keywords': ['keywords', 'tags'],
            'instruments': ['instruments', 'instrument', 'ms_instrument'],
            'submissionDate': ['submission_date', 'submitted', 'date_submitted', 'submissionDate'],
            'publicationDate': ['publication_date', 'published', 'date_published', 'publicationDate'],
            'doi': ['doi', 'publication_doi'],
            'submitters': ['submitters', 'authors', 'contact']
        }
        
        self.output_columns = [
            'accession',
            'title', 
            'projectDescription',
            'keywords',
            'instruments',
            'submissionDate',
            'publicationDate',
            'doi',
            'submitters'
        ]

    def find_json_objects_streaming(self, filename: str) -> Iterator[str]:
        """
        Find JSON objects in the file using a streaming approach.
        
        Args:
            filename: Path to the JSON file
            
        Yields:
            JSON object strings
        """
        print(f"Streaming through JSON file: {filename}")
        
        with open(filename, 'r', encoding='utf-8') as f:
            buffer = ""
            brace_count = 0
            in_string = False
            escape_next = False
            
            for line in f:
                for char in line:
                    if escape_next:
                        escape_next = False
                        buffer += char
                        continue
                    
                    if char == '\\':
                        escape_next = True
                        buffer += char
                        continue
                    
                    if char == '"' and not escape_next:
                        in_string = not in_string
                        buffer += char
                        continue
                    
                    if not in_string:
                        if char == '{':
                            if brace_count == 0:
                                buffer = char
                            else:
                                buffer += char
                            brace_count += 1
                        elif char == '}':
                            buffer += char
                            brace_count -= 1
                            if brace_count == 0:
                                # Complete JSON object found
                                yield buffer.strip()
                                buffer = ""
                        else:
                            if brace_count > 0:
                                buffer += char
                    else:
                        buffer += char
            
            # Handle any remaining buffer
            if buffer.strip():
                yield buffer.strip()

    def parse_json_object(self, json_str: str) -> Optional[Dict[str, Any]]:
        """
        Parse a single JSON object string.
        
        Args:
            json_str: JSON object string
            
        Returns:
            Parsed dictionary or None if parsing fails
        """
        try:
            # Clean the JSON string
            cleaned = self.clean_json_string(json_str)
            
            # Try to parse
            obj = json.loads(cleaned)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            # Try to fix common issues
            try:
                fixed = self.fix_json_object(json_str)
                obj = json.loads(fixed)
                if isinstance(obj, dict):
                    return obj
            except json.JSONDecodeError:
                pass
        
        return None

    def clean_json_string(self, json_str: str) -> str:
        """
        Clean up JSON string.
        
        Args:
            json_str: Raw JSON string
            
        Returns:
            Cleaned JSON string
        """
        # Remove null bytes and other problematic characters
        json_str = json_str.replace('\x00', '')
        
        # Fix common JSON issues
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)
        
        # Remove trailing commas
        json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
        
        return json_str.strip()

    def fix_json_object(self, json_str: str) -> str:
        """
        Try to fix common JSON formatting issues.
        
        Args:
            json_str: JSON string to fix
            
        Returns:
            Fixed JSON string
        """
        # Remove trailing commas
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)
        
        # Fix unquoted keys (basic approach)
        json_str = re.sub(r'(\w+):', r'"\1":', json_str)
        
        # Fix single quotes
        json_str = json_str.replace("'", '"')
        
        # Remove extra whitespace
        json_str = json_str.strip()
        
        return json_str

    def extract_field_value(self, dataset: Dict[str, Any], field_name: str) -> str:
        """
        Extract field value from dataset using multiple possible field names.
        
        Args:
            dataset: Dataset dictionary
            field_name: Target field name
            
        Returns:
            Extracted value as string
        """
        if field_name not in self.target_fields:
            return ""
        
        # Try all possible field names for this field
        for possible_field in self.target_fields[field_name]:
            if possible_field in dataset and dataset[possible_field]:
                value = dataset[possible_field]
                return self.format_field_value(value, field_name)
        
        return ""

    def format_field_value(self, value: Any, field_name: str) -> str:
        """
        Format field value for TSV output.
        
        Args:
            value: Raw field value
            field_name: Name of the field
            
        Returns:
            Formatted string value
        """
        if value is None:
            return ""
        
        if isinstance(value, str):
            # Clean up the string
            return value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ').strip()
        
        elif isinstance(value, list):
            if field_name == 'instruments':
                # Special handling for instruments - extract instrument names
                instrument_names = []
                for instrument in value:
                    if isinstance(instrument, dict):
                        if 'name' in instrument:
                            instrument_names.append(instrument['name'])
                        elif 'accession' in instrument:
                            instrument_names.append(instrument['accession'])
                    elif isinstance(instrument, str):
                        instrument_names.append(instrument)
                return "; ".join(instrument_names)
            
            elif field_name == 'keywords':
                # Join keywords with semicolon
                return "; ".join(str(item) for item in value)
            
            elif field_name == 'submitters':
                # Extract submitter information
                submitter_info = []
                for submitter in value:
                    if isinstance(submitter, dict):
                        name_parts = []
                        if 'firstName' in submitter:
                            name_parts.append(submitter['firstName'])
                        if 'lastName' in submitter:
                            name_parts.append(submitter['lastName'])
                        if name_parts:
                            submitter_info.append(" ".join(name_parts))
                        elif 'name' in submitter:
                            submitter_info.append(submitter['name'])
                    elif isinstance(submitter, str):
                        submitter_info.append(submitter)
                return "; ".join(submitter_info)
            
            else:
                # Default list handling
                return "; ".join(str(item) for item in value)
        
        elif isinstance(value, dict):
            # For dictionary values, try to extract meaningful information
            if 'name' in value:
                return str(value['name'])
            elif 'title' in value:
                return str(value['title'])
            else:
                return str(value)
        
        else:
            return str(value)

    def convert_to_tsv(self, input_filename: str, output_filename: str) -> bool:
        """
        Convert JSON file to TSV format using streaming parsing.
        
        Args:
            input_filename: Input JSON file
            output_filename: Output TSV file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"Converting {input_filename} to {output_filename}")
            print("=" * 50)
            
            with open(output_filename, 'w', newline='', encoding='utf-8') as tsvfile:
                writer = csv.writer(tsvfile, delimiter='\t')
                
                # Write header
                writer.writerow(self.output_columns)
                
                # Process datasets
                processed_count = 0
                valid_count = 0
                
                for json_obj_str in self.find_json_objects_streaming(input_filename):
                    processed_count += 1
                    
                    if processed_count % 1000 == 0:
                        print(f"Found {processed_count} objects, parsed {valid_count} successfully...")
                    
                    # Parse the JSON object
                    dataset = self.parse_json_object(json_obj_str)
                    if dataset:
                        valid_count += 1
                        
                        # Extract values for each column
                        row = []
                        for column in self.output_columns:
                            value = self.extract_field_value(dataset, column)
                            row.append(value)
                        
                        writer.writerow(row)
                
                print(f"Processing complete!")
                print(f"Total objects found: {processed_count}")
                print(f"Successfully parsed: {valid_count}")
                print(f"Successfully converted {valid_count} datasets to {output_filename}")
                return True
                
        except Exception as e:
            print(f"Error converting to TSV: {e}")
            return False

def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python streaming_json_parser.py <json_file> [output_file]")
        print("  <json_file>: Path to the input JSON file")
        print("  [output_file]: Optional output TSV file path")
        return
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    if output_file is None:
        output_file = input_file.replace('.json', '_streaming.tsv')
    
    parser = StreamingJSONParser()
    success = parser.convert_to_tsv(input_file, output_file)
    
    if success:
        print(f"\nConversion complete!")
        print(f"Input file: {input_file}")
        print(f"Output file: {output_file}")
    else:
        print("Conversion failed.")
        sys.exit(1)

if __name__ == "__main__":
    main() 
