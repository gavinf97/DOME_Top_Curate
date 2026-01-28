import os
import json
import glob
import pandas as pd
from collections import Counter

# Paths
HUMAN_EVAL_DIR = '../30_human_evaluation'
FLATTENED_SOURCE = '../DOME_Registry_JSON_Files/flattened_DOME_Registry_Contents_2026-01-09.json'
OUTPUT_REPORT = 'consistency_report.txt'

def normalize_doi(doi):
    if not doi or doi == '-':
        return ""
    return doi.strip().lower()

def get_keys_recursively(d, parent_key='', sep='/'):
    keys = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            keys.extend(get_keys_recursively(v, new_key, sep=sep))
        else:
            keys.append(new_key)
    return keys

def main():
    print("Loading Flattened Source Data...")
    try:
        with open(FLATTENED_SOURCE, 'r') as f:
            flattened_data = json.load(f)
    except Exception as e:
        print(f"Error loading flattened source: {e}")
        return

    # Map DOI to Flattened Entry
    flattened_map = {}
    for entry in flattened_data:
        doi = entry.get('publication_doi')
        if doi:
            flattened_map[normalize_doi(doi)] = entry
    
    print(f"Loaded {len(flattened_map)} entries from flattened source.")

    # Load 30 Human Eval Files
    human_files = glob.glob(os.path.join(HUMAN_EVAL_DIR, 'PMC*', '*_human.json'))
    print(f"Found {len(human_files)} human evaluation JSON files.")

    if not human_files:
        print("No files found to analyze.")
        return

    # Analyze Consistency
    report = []
    
    field_presence_counts = Counter()
    total_files = 0
    
    file_analysis = []

    for file_path in human_files:
        total_files += 1
        pmc_id = os.path.basename(os.path.dirname(file_path))
        
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Flatten keys for analysis
        keys = set(get_keys_recursively(data))
        field_presence_counts.update(keys)
        
        doi = data.get('publication', {}).get('doi')
        normalized_doi = normalize_doi(doi)
        
        source_entry = flattened_map.get(normalized_doi)
        
        file_analysis.append({
            'pmc_id': pmc_id,
            'file_path': file_path,
            'keys': keys,
            'doi': normalized_doi,
            'source_entry': source_entry
        })

    # Generate Report
    report.append("=== Consistency Analysis of 30 Human Evaluation JSONs ===\n")
    report.append(f"Total Files Analyzed: {total_files}\n")
    
    # 1. Field Consistency
    report.append("\n--- Field Presence Frequency (Across 30 Files) ---\n")
    all_possible_keys = list(field_presence_counts.keys())
    all_possible_keys.sort()
    
    missing_fields_summary = {} # key -> list of PMCIDs missing it

    for key in all_possible_keys:
        count = field_presence_counts[key]
        if count < total_files:
            missing_in = []
            for item in file_analysis:
                if key not in item['keys']:
                    missing_in.append(item['pmc_id'])
            missing_fields_summary[key] = missing_in
            report.append(f"[{count}/{total_files}] {key} (Missing in {len(missing_in)} files)\n")
    
    report.append(f"\nThere are {len([k for k,v in field_presence_counts.items() if v == total_files])} consistent fields present in all files.\n")

    # 2. Check Disparities vs Flattened Source
    report.append("\n--- Disparity Check vs Flattened Source (Why are fields missing?) ---\n")
    
    for key, pmc_list in missing_fields_summary.items():
        if not pmc_list: continue
        
        # We only look at a few examples per missing key
        example_pmc = pmc_list[0] 
        example_item = next(item for item in file_analysis if item['pmc_id'] == example_pmc)
        
        report.append(f"\nField '{key}' is missing in {example_pmc} (and {len(pmc_list)-1} others).")
        source = example_item['source_entry']
        
        if source:
            # Construct expected flattened key name
            # Replace ONLY the first slash with underscore
            # matches/optimization/algorithm -> matches_optimization/algorithm
            if '/' in key:
                parts = key.split('/', 1)
                flat_key_guess = f"{parts[0]}_{parts[1]}"
            else:
                flat_key_guess = key

            if flat_key_guess in source:
                source_value = source[flat_key_guess]
                if source_value is None or source_value == "" or source_value == "-" or source_value == "N/A":
                    report.append(f"  -> In Reference Source: Present but empty/null value: '{source_value}'")
                elif source_value == 0: # Handle numeric 0 if relevant
                    report.append(f"  -> In Reference Source: VALUE EXISTS! '{source_value}' (Value is 0)")
                else:
                    report.append(f"  -> In Reference Source: VALUE EXISTS! '{source_value}'\n  -> Disparity: PROBABLY NOT PROPAGATED during creation of human json or source structure mismatch.")
            else:
                 report.append(f"  -> In Reference Source: NOT FOUND (Key '{flat_key_guess}' not in source)")
        else:
            report.append("  -> Reference Source Entry NOT FOUND for this DOI.")

    with open(OUTPUT_REPORT, 'w') as f:
        f.writelines(report)
    
    print(f"Analysis complete. Report written to {OUTPUT_REPORT}")
    with open(OUTPUT_REPORT, 'r') as f:
        print(f.read())

if __name__ == "__main__":
    main()
