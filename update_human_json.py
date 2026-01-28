import json
import os
import glob

# Paths
workspace_root = "/home/gavinfarrell/PhD_Code/DOME_Top_Curate"
source_json_path = os.path.join(workspace_root, "DOME_Registry_JSON_Files/dome_review_raw_human_20260128.json")
eval_dir = os.path.join(workspace_root, "30_human_evaluation")

def normalize_doi(doi):
    if not doi:
        return ""
    return doi.strip().lower()

def flatten_record(record):
    flat = {}
    categories = ["publication", "dataset", "optimization", "model", "evaluation"]
    
    for cat in categories:
        if cat in record:
            for key, value in record[cat].items():
                if key in ["done", "skip", "pmid", "updated", "_id", "user"]: # Skip metadata fields if any (pmid/updated observed in source)
                    # Wait, pmid might be useful but copilot.json didn't seem to rely on it as a primary key, but copilot.json had 'publication/year', 'publication/authors' etc.
                    # Let's keep data fields. pmid is metadata but maybe useful? The user asked to be consistent with copilot.json.
                    # copilot.json fields:
                    # publication/title, authors, journal, year, doi, tags
                    # dataset/provenance, splits, redundancy, availability
                    # optimization/algorithm, meta, encoding, parameters, features, fitting, regularization, config
                    # model/interpretability, output, duration, availability
                    # evaluation/method, measure, comparison, confidence, availability
                    
                    # The source has keys like 'pmid', 'updated' in 'publication'. copilot.json doesn't seem to have pmid.
                    # I will exclude keys that are not in the standard set if possible, or just include all and let the user decide?
                    # "reformat so fields consistent against the copilot.json".
                    # I should probably try to only include keys that appear in the 'standard' schema or map them.
                    # However, source keys seem to match well.
                    # I will exclude 'done', 'skip', 'updated'. I will keep 'pmid' if it exists just in case, or drop it. 
                    # Looking at copilot.json, it DOES NOT have pmid. So I will drop pmid.
                    continue
                
                flat[f"{cat}/{key}"] = value
    return flat

def main():
    print(f"Loading source data from {source_json_path}...")
    with open(source_json_path, 'r') as f:
        source_data = json.load(f)
    
    # Create DOI map
    doi_map = {}
    for entry in source_data:
        if 'publication' in entry and 'doi' in entry['publication']:
            doi = normalize_doi(entry['publication']['doi'])
            if doi:
                doi_map[doi] = entry
    
    print(f"Loaded {len(doi_map)} records with DOIs.")
    
    # Iterate over evaluation directories
    subdirs = [d for d in os.listdir(eval_dir) if os.path.isdir(os.path.join(eval_dir, d))]
    
    for subdir in sorted(subdirs):
        dir_path = os.path.join(eval_dir, subdir)
        
        # Find copilot file to get DOI
        copilot_files = glob.glob(os.path.join(dir_path, "*_copilot.json"))
        target_human_file = os.path.join(dir_path, f"{subdir}_human.json")
        
        if not copilot_files:
            print(f"Skipping {subdir}: No copilot.json found.")
            continue
            
        copilot_file = copilot_files[0]
        
        try:
            with open(copilot_file, 'r') as f:
                copilot_data = json.load(f)
            
            doi = copilot_data.get("publication/doi")
            if not doi:
                print(f"Skipping {subdir}: No DOI in copilot file.")
                continue
                
            norm_doi = normalize_doi(doi)
            
            if norm_doi in doi_map:
                source_entry = doi_map[norm_doi]
                new_human_data = flatten_record(source_entry)
                
                # Check consistency?
                # The user said "reformat so fields consistent against the copilot.json"
                # The flatten_record function does this by creating "category/key" structure.
                
                # Write to human.json
                with open(target_human_file, 'w') as f:
                    json.dump(new_human_data, f, indent=2)
                print(f"Updated {subdir} (DOI: {doi})")
            else:
                print(f"Warning: No match found for {subdir} (DOI: {doi}) in source data.")
                
        except Exception as e:
            print(f"Error processing {subdir}: {e}")

if __name__ == "__main__":
    main()
