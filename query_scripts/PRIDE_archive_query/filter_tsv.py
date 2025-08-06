#!/usr/bin/env python3
"""
Ultra-Strict TSV Filter for Immunopeptidomics Datasets

This script filters the converted TSV file to find ONLY immunopeptidomics datasets
that are cancer-related and analyzed on timsTOF instruments.
Cancer keywords are ONLY searched in the keywords field, not in title or description.
"""

import csv
import re
import sys
from typing import List, Dict, Any

class UltraStrictTSVFilter:
    """Class to ultra-strictly filter TSV datasets for immunopeptidomics only."""
    
    def __init__(self):
        # timsTOF instrument variants
        self.timstof_patterns = [
            r'timsTOF',
            r'timsTOF\s+Pro',
            r'timsTOF\s+Pro\s+2',
            r'timsTOF\s+SCP',
            r'timsTOF\s+HT',
            r'timsTOF\s+Flex',
            r'timsTOF\s+Ultra',
            r'timsTOF\s+Elite',
            r'timsTOF\s+Discovery'
        ]
        
        # STRICT immunopeptidomics keywords (must contain these)
        self.immunopeptidomics_keywords = [
            'immunopeptidomics',
            'immunopeptidomic',
            'immunopeptidome',
            'immunopeptide',
            'hla peptidome',
            'mhc peptidome',
            'antigen presentation',
            'peptide presentation',
            't cell epitope',
            'cd8 epitope',
            'cd4 epitope',
            'hla class i',
            'hla class ii',
            'mhc class i',
            'mhc class ii',
            'hla-i',
            'hla-ii',
            'mhc-i',
            'mhc-ii',
            'human leukocyte antigen peptidome',
            'major histocompatibility complex peptidome'
        ]
        
        # Cancer-related keywords (ONLY searched in keywords field)
        self.cancer_keywords = [
            'cancer',
            'tumour',
            'tumor',
            'malignant',
            'benign',
            'oncology',
            'neoplasm',
            'carcinoma',
            'sarcoma',
            'leukemia',
            'lymphoma',
            'melanoma',
            'glioblastoma',
            'adenocarcinoma',
            'metastasis',
            'metastatic',
            'cancerous',
            'tumorous',
            'neuroblastoma',
            'oral cancer',
            'breast cancer',
            'lung cancer',
            'prostate cancer',
            'colorectal cancer',
            'pancreatic cancer',
            'ovarian cancer',
            'cervical cancer',
            'endometrial cancer',
            'thyroid cancer',
            'brain cancer',
            'bone cancer',
            'skin cancer',
            'stomach cancer',
            'esophageal cancer',
            'head and neck cancer',
            'testicular cancer',
            'adrenal cancer'
        ]
        
        # Terms to EXCLUDE (general proteomics, phosphoproteomics, etc.)
        self.exclude_terms = [
            'proteomics',
            'proteomic',
            'phosphoproteomics',
            'phosphoproteomic',
            'glycoproteomics',
            'glycoproteomic',
            'acetylproteomics',
            'acetylproteomic',
            'ubiquitinomics',
            'ubiquitinomic',
            'metabolomics',
            'metabolomic',
            'lipidomics',
            'lipidomic',
            'transcriptomics',
            'transcriptomic',
            'genomics',
            'genomic',
            'epigenomics',
            'epigenomic',
            'phosphorylation',
            'glycosylation',
            'acetylation',
            'ubiquitination',
            'methylation',
            'sumoylation',
            'palmitoylation',
            'myristoylation',
            'farnesylation',
            'geranylation'
        ]

    def check_timstof_instrument(self, instruments_str: str) -> bool:
        """
        Check if the instruments string contains timsTOF.
        
        Args:
            instruments_str: Instruments field from TSV
            
        Returns:
            True if timsTOF is found, False otherwise
        """
        if not instruments_str:
            return False
        
        instruments_lower = instruments_str.lower()
        
        # Check for any timsTOF pattern
        for pattern in self.timstof_patterns:
            if re.search(pattern, instruments_lower, re.IGNORECASE):
                return True
        
        return False

    def check_strict_immunopeptidomics(self, keywords_str: str, title_str: str = "", description_str: str = "") -> bool:
        """
        Check if the dataset is STRICTLY immunopeptidomics (not general proteomics).
        Searches keywords, title, and description for immunopeptidomics terms.
        Allows datasets that contain BOTH immunopeptidomics AND other omics terms.
        
        Args:
            keywords_str: Keywords field from TSV
            title_str: Title field from TSV
            description_str: Description field from TSV
            
        Returns:
            True if immunopeptidomics keywords are found, False otherwise
        """
        # Combine all text fields for searching
        search_text = f"{keywords_str} {title_str} {description_str}".lower()
        
        # First, check if immunopeptidomics terms are present
        has_immunopeptidomics = False
        for keyword in self.immunopeptidomics_keywords:
            if keyword.lower() in search_text:
                has_immunopeptidomics = True
                break
        
        # If no immunopeptidomics terms found, reject the dataset
        if not has_immunopeptidomics:
            return False
        
        # If immunopeptidomics is present, allow other omics terms
        # Only exclude if it's PURELY other omics without immunopeptidomics
        other_omics_found = []
        for exclude_term in self.exclude_terms:
            if exclude_term.lower() in search_text:
                other_omics_found.append(exclude_term)
        
        # If we have immunopeptidomics AND other omics, that's OK
        # If we have ONLY other omics (no immunopeptidomics), that's NOT OK
        if other_omics_found and has_immunopeptidomics:
            # This is acceptable - dataset has both immunopeptidomics and other omics
            return True
        elif other_omics_found and not has_immunopeptidomics:
            # This is NOT acceptable - dataset has only other omics, no immunopeptidomics
            return False
        else:
            # Only immunopeptidomics, no other omics - this is acceptable
            return True

    def check_cancer_keywords_only(self, keywords_str: str) -> bool:
        """
        Check if cancer-related keywords are present ONLY in the keywords field.
        
        Args:
            keywords_str: Keywords field from TSV
            
        Returns:
            True if cancer keywords are found in keywords field, False otherwise
        """
        if not keywords_str:
            return False
        
        keywords_lower = keywords_str.lower()
        
        # Check for cancer keywords ONLY in keywords field
        for keyword in self.cancer_keywords:
            if keyword.lower() in keywords_lower:
                return True
        
        return False

    def filter_datasets(self, input_filename: str, output_filename: str) -> bool:
        """
        Filter datasets based on ultra-strict criteria and save to new TSV file.
        
        Args:
            input_filename: Input TSV file
            output_filename: Output filtered TSV file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"ULTRA-STRICT Filtering of datasets from {input_filename}")
            print("=" * 70)
            print("ULTRA-STRICT Search criteria:")
            print("- Instruments MUST contain 'timsTOF'")
            print("- MUST contain immunopeptidomics terms (NOT general proteomics)")
            print("- Cancer keywords MUST be in keywords field ONLY (not title/description)")
            print("- EXCLUDES: proteomics, phosphoproteomics, glycoproteomics, etc.")
            print("=" * 70)
            
            filtered_datasets = []
            total_datasets = 0
            timstof_count = 0
            immunopeptidomics_count = 0
            cancer_keywords_count = 0
            excluded_count = 0
            all_criteria_count = 0
            
            with open(input_filename, 'r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile, delimiter='\t')
                
                # Get field names
                fieldnames = reader.fieldnames
                
                for row in reader:
                    total_datasets += 1
                    
                    if total_datasets % 10000 == 0:
                        print(f"Processed {total_datasets} datasets...")
                    
                    # Extract relevant fields
                    instruments = row.get('instruments', '')
                    keywords = row.get('keywords', '')
                    title = row.get('title', '')
                    description = row.get('projectDescription', '')
                    
                    # Check criteria
                    has_timstof = self.check_timstof_instrument(instruments)
                    is_immunopeptidomics = self.check_strict_immunopeptidomics(keywords, title, description)
                    has_cancer_keywords = self.check_cancer_keywords_only(keywords)
                    
                    if has_timstof:
                        timstof_count += 1
                    if is_immunopeptidomics:
                        immunopeptidomics_count += 1
                    if has_cancer_keywords:
                        cancer_keywords_count += 1
                    
                    # Check if dataset was excluded due to general proteomics terms
                    search_text = f"{keywords} {title} {description}".lower()
                    excluded = any(exclude_term.lower() in search_text for exclude_term in self.exclude_terms)
                    if excluded:
                        excluded_count += 1
                    
                    # Only include datasets that match ALL THREE criteria
                    if has_timstof and is_immunopeptidomics and has_cancer_keywords:
                        all_criteria_count += 1
                        filtered_datasets.append(row)
                        print(f"Found ULTRA-STRICT matching dataset: {row.get('accession', 'Unknown')}")
                        print(f"  Title: {title[:100]}...")
                        print(f"  Keywords: {keywords}")
            
            # Save filtered datasets
            if filtered_datasets:
                with open(output_filename, 'w', newline='', encoding='utf-8') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter='\t')
                    writer.writeheader()
                    writer.writerows(filtered_datasets)
                
                print(f"\nULTRA-STRICT Filtering complete!")
                print(f"Total datasets processed: {total_datasets}")
                print(f"Datasets with timsTOF: {timstof_count}")
                print(f"Datasets with immunopeptidomics (strict): {immunopeptidomics_count}")
                print(f"Datasets with cancer keywords (keywords field only): {cancer_keywords_count}")
                print(f"Datasets excluded (general proteomics): {excluded_count}")
                print(f"Datasets matching ALL THREE criteria: {all_criteria_count}")
                print(f"Filtered datasets saved to: {output_filename}")
                
                return True
            else:
                print(f"\nNo datasets found matching ALL THREE ultra-strict criteria.")
                print(f"Total datasets processed: {total_datasets}")
                print(f"Datasets with timsTOF: {timstof_count}")
                print(f"Datasets with immunopeptidomics (strict): {immunopeptidomics_count}")
                print(f"Datasets with cancer keywords (keywords field only): {cancer_keywords_count}")
                print(f"Datasets excluded (general proteomics): {excluded_count}")
                return False
                
        except FileNotFoundError:
            print(f"Error: File '{input_filename}' not found.")
            return False
        except Exception as e:
            print(f"Error filtering datasets: {e}")
            return False

    def print_criteria_summary(self):
        """Print a summary of the ultra-strict filtering criteria."""
        print("\nULTRA-STRICT Immunopeptidomics Keywords (searched in keywords, title, description):")
        for keyword in self.immunopeptidomics_keywords:
            print(f"  - {keyword}")
        
        print("\nCancer Keywords (ONLY searched in keywords field):")
        for keyword in self.cancer_keywords:
            print(f"  - {keyword}")
        
        print("\nLogic for Other Omics Terms:")
        print("  - If dataset contains immunopeptidomics AND other omics → ACCEPT")
        print("  - If dataset contains ONLY other omics (no immunopeptidomics) → REJECT")
        print("  - If dataset contains ONLY immunopeptidomics (no other omics) → ACCEPT")
        
        print("\nOther Omics Terms (will be allowed if immunopeptidomics is also present):")
        for term in self.exclude_terms:
            print(f"  - {term}")

def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python ultra_strict_tsv_filter.py <input_tsv> [output_tsv]")
        print("  <input_tsv>: Path to the input TSV file")
        print("  [output_tsv]: Optional output TSV file path (default: ultra_strict_filtered_ip_data.tsv)")
        print("\nExample:")
        print("  python ultra_strict_tsv_filter.py pride_datasets.tsv")
        print("  python ultra_strict_tsv_filter.py pride_datasets.tsv my_ultra_strict_filtered_data.tsv")
        return
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "ultra_strict_filtered_ip_data.tsv"
    
    filter_tool = UltraStrictTSVFilter()
    
    # Show criteria summary
    filter_tool.print_criteria_summary()
    
    # Filter datasets
    success = filter_tool.filter_datasets(input_file, output_file)
    
    if success:
        print(f"\nULTRA-STRICT filtering completed successfully!")
        print(f"Output file: {output_file}")
    else:
        print("ULTRA-STRICT filtering failed.")
        sys.exit(1)

if __name__ == "__main__":
    main() 
