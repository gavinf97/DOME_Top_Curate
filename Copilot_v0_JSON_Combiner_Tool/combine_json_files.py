import os
import json
import glob

def combine_json_files(source_dir, output_file):
    """
    Combines all JSON files from the source directory into a single JSON list.
    """
    
    # 1. Verify source directory exists
    if not os.path.exists(source_dir):
        print(f"Error: Source directory not found at {source_dir}")
        return

    # 2. Find all JSON files
    json_pattern = os.path.join(source_dir, "*.json")
    files = glob.glob(json_pattern)
    files.sort() # Sort to ensure deterministic order
    
    print(f"Found {len(files)} JSON files in {source_dir}")
    
    combined_data = []
    
    # 3. Iterate and read files
    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Check if data is a dict (single entry) or list (multiple entries)
                # The individual files seem to be single objects representing one paper
                if isinstance(data, dict):
                    # Optional: Add filename as metadata if needed, but not requested
                    # data['_source_file'] = os.path.basename(file_path)
                    combined_data.append(data)
                elif isinstance(data, list):
                    combined_data.extend(data)
                else:
                    print(f"Warning: Skipping {os.path.basename(file_path)} - Unknown format")
                    
        except json.JSONDecodeError as e:
            print(f"Error decoding {os.path.basename(file_path)}: {e}")
        except Exception as e:
            print(f"Error reading {os.path.basename(file_path)}: {e}")

    # 4. Write combined output
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(combined_data, f, indent=4)
        print(f"Successfully wrote {len(combined_data)} records to {output_file}")
    except Exception as e:
        print(f"Error writing output file: {e}")

def main():
    # Configuration
    # Using absolute paths to be safe, or relative to this script location
    
    # This script is in: /home/gavinfarrell/PhD_Code/DOME_Top_Curate/Copilot_v0_JSON_Combiner_Tool
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Define tasks: (Source Directory, Output Filename)
    tasks = [
        (
            os.path.join(project_root, "Copilot_v0_Processed_2025-12-04", "registry_v0"), 
            os.path.join(script_dir, "combined_copilot_v0.json")
        ),
        (
            os.path.join(project_root, "Copilot_1000_v0_Processed_2026-01-15_Updated_Metadata"),
            os.path.join(script_dir, "combined_copilot_1000_v0_updated.json")
        ),
        (
            os.path.join(project_root, "Copilot_v0_Processed_2025-12-04_Updated_Metadata"),
            os.path.join(script_dir, "combined_copilot_v0_updated.json")
        )
    ]
    
    print("--- JSON Combiner Tool ---")
    
    for source_dir, output_file in tasks:
        print(f"\nProcessing Task:")
        print(f"  Source: {source_dir}")
        print(f"  Output: {output_file}")
        
        # Check if source dir exists before running
        if os.path.exists(source_dir):
            combine_json_files(source_dir, output_file)
        else:
            print(f"  Error: Source directory does not exist. Skipping.")

if __name__ == "__main__":
    main()
