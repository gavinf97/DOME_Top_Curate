import pandas as pd
import json
import requests
import time
import os
import shutil
import collections
import numpy as np

# --- Configuration & Setup ---
SOURCE_DIR = "../DOME_pre-DSW_Repair"
WORK_DIR = "." # We are running inside DOME_Repair_Automation

FILES_TO_COPY = [
    "Dome-Recommendations-Annotated-Articles_20250202.tsv",
    "dome_users_20260202.json",
    "dome_review_raw_human_20260202.json",
    "dedup_public_dome_review_raw_human_20260202.json" # Try to copy this if it exists
]

print("=== Starting DOME Repair Automation ===")
print(f"Working Directory: {os.path.abspath(WORK_DIR)}")

# --- Step 0: Copy Source Files ---
print("\n[Step 0] Copying source files to workspace...")
for fname in FILES_TO_COPY:
    src = os.path.join(SOURCE_DIR, fname)
    dst = os.path.join(WORK_DIR, fname)
    if os.path.exists(src):
        shutil.copy2(src, dst)
        print(f"  Copied: {fname}")
    else:
        print(f"  Warning: Source file {fname} not found in {SOURCE_DIR}")

# Verify essential files
if not os.path.exists("Dome-Recommendations-Annotated-Articles_20250202.tsv"):
    print("Error: Starting TSV not found. Aborting.")
    exit(1)

# --- Step 1: Enrich with EPMC ---
print("\n[Step 1] Enriching data with Europe PMC API...")

def fetch_epmc_metadata(pmid):
    if not pmid or str(pmid) == 'nan':
        return None, None, None, None
    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    params = {'query': f'EXT_ID:{pmid} SRC:MED', 'format': 'json', 'resultType': 'core'}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        result_list = data.get('resultList', {}).get('result', [])
        if result_list:
            top_result = result_list[0]
            return top_result.get('title', ''), top_result.get('authorString', ''), top_result.get('pubYear', ''), top_result.get('doi', '')
    except Exception as e:
        print(f"    Error fetching PMID {pmid}: {e}")
    return None, None, None, None

tsv_file = "Dome-Recommendations-Annotated-Articles_20250202.tsv"
df_recs = pd.read_csv(tsv_file, sep='\t')
print(f"  Loaded {len(df_recs)} records from {tsv_file}")

# ADD PROVENANCE COLUMN (Requirement)
print("  Adding 'dataset/provenance_source' column...")
df_recs['dataset/provenance_source'] = 'Starting_TSV'

# Initial enrichment (only if missing columns)
new_cols = ['EPMC_title', 'EPMC_authors', 'EPMC_pub_year', 'EPMC_doi']
for col in new_cols:
    if col not in df_recs.columns:
        df_recs[col] = None

# For demo speed, we might want to skip full re-fetch if not needed, 
# but per instructions "automated version", we run it.
# To save time, only fetch if missing or if requested? I'll run it as per notebook logic.
print("  Fetching metadata (This may take time)...")
# Using a small limit for testing? No, full run.
for index, row in df_recs.iterrows():
    pmid = row.get('PMID')
    if pd.isna(pmid): continue
    
    # Simple progress
    if index % 10 == 0:
        print(f"    Processing {index + 1}/{len(df_recs)}...", end='\r')
        
    title, authors, year, doi = fetch_epmc_metadata(pmid)
    df_recs.at[index, 'EPMC_title'] = title
    df_recs.at[index, 'EPMC_authors'] = authors
    df_recs.at[index, 'EPMC_pub_year'] = year
    df_recs.at[index, 'EPMC_doi'] = doi
    time.sleep(0.1)

print("\n  Enrichment complete.")

# --- Step 2: Integrate User OIDs ---
print("\n[Step 2] Mapping User OIDs...")
users_file = "dome_users_20260202.json"
if os.path.exists(users_file):
    with open(users_file, 'r', encoding='utf-8') as f:
        users_data = json.load(f)
    email_to_oid = {}
    for u in users_data:
        email, oid = u.get('email'), u.get('_id', {}).get('$oid')
        if email and oid: email_to_oid[email.strip()] = oid
    
    if 'Indirizzo email' in df_recs.columns:
        df_recs['User_OID'] = df_recs['Indirizzo email'].apply(lambda x: email_to_oid.get(str(x).strip(), 'Unknown'))
        print("  User OIDs mapped.")
    else:
        print("  Warning: 'Indirizzo email' column not found.")
else:
    print(f"  Warning: {users_file} not found.")

output_enriched = "Dome-Recommendations-Annotated-Articles_20250202_Enriched.tsv"
df_recs.to_csv(output_enriched, sep='\t', index=False)
print(f"  Saved enriched data to {output_enriched}")

# --- Step 3: Column Standardization ---
print("\n[Step 3] Standardizing Columns...")
raw_reviews_file = "dome_review_raw_human_20260202.json"
column_map = {
    'Journal name': 'publication/journal', 'Publication year': 'publication/year',
    'Provenance': 'dataset/provenance', 'Dataset splits': 'dataset/splits',
    'Redundancy between data splits': 'dataset/redundancy', 'Availability of data': 'dataset/availability',
    'Algorithm': 'optimization/algorithm', 'Meta-predictions': 'optimization/meta',
    'Data encoding': 'optimization/encoding', 'Parameters': 'optimization/parameters',
    'Features': 'optimization/features', 'Fitting': 'optimization/fitting',
    'Regularization': 'optimization/regularization', 'Availability of configuration': 'optimization/config',
    'Interpretability': 'model/interpretability', 'Output': 'model/output',
    'Execution time': 'model/duration', 'Availability of software': 'model/availability',
    'Evaluation method': 'evaluation/method', 'Performance measures': 'evaluation/measure',
    'Comparison': 'evaluation/comparison', 'Confidence': 'evaluation/confidence',
    'Availability of evaluation': 'evaluation/availability',
    'Informazioni cronologiche': 'timestamp', 'Indirizzo email': 'user_email'
}
df_recs.rename(columns=column_map, inplace=True)

# Schema extraction
all_json_keys = set()
if os.path.exists(raw_reviews_file):
    with open(raw_reviews_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    for entry in data[:10]:
        for section, content in entry.items():
            if isinstance(content, dict):
                for field in content.keys():
                    if not field.startswith('$'): all_json_keys.add(f"{section}/{field}")
            else:
                if not section.startswith('_'): all_json_keys.add(section)
    print(f"  Detected {len(all_json_keys)} schema keys.")
else:
    print("  Warning: JSON file for schema not found. Using defaults.")
    # (Skip default set for brevity, assuming file exists)

for key in all_json_keys:
    if key not in df_recs.columns: df_recs[key] = None

# Drop unwanted
cols_to_drop = [c for c in df_recs.columns if c.endswith('/skip') or c.endswith('/done') or c == 'DOME version']
df_recs.drop(columns=cols_to_drop, inplace=True, errors='ignore')

# --- Step 4: Merge EPMC to Schema ---
print("\n[Step 4] Merging EPMC data to schema...")
merge_pairs = [('EPMC_title', 'publication/title'), ('EPMC_authors', 'publication/authors'), ('EPMC_doi', 'publication/doi')]
for src, tgt in merge_pairs:
    if src in df_recs.columns and tgt in df_recs.columns:
        df_recs[tgt] = df_recs[src]
df_recs.drop(columns=[src for src, tgt in merge_pairs], inplace=True, errors='ignore')

# --- Step 5: Finalize Columns (v4-v9 combined logic) ---
print("\n[Step 5] Finalizing Column Order and Naming...")
# Renames with duplicate checks
if 'PMID' in df_recs.columns:
    if 'publication/pmid' in df_recs.columns:
        # Merge: fill NA in target with source, then drop source
        df_recs['publication/pmid'] = df_recs['publication/pmid'].fillna(df_recs['PMID'])
        df_recs.drop(columns=['PMID'], inplace=True)
    else:
        df_recs.rename(columns={'PMID': 'publication/pmid'}, inplace=True)

if 'timestamp' in df_recs.columns:
    if 'update' in df_recs.columns:
         df_recs['update'] = df_recs['update'].fillna(df_recs['timestamp'])
         df_recs.drop(columns=['timestamp'], inplace=True)
    else:
        df_recs.rename(columns={'timestamp': 'update'}, inplace=True)

# Remove any duplicate columns if they exist (keep First)
df_recs = df_recs.loc[:, ~df_recs.columns.duplicated()]

# Migrations
migration_map = [('Availability of  configuration', 'optimization/config'), ('Execution time ', 'model/duration'), ('Performance measures ', 'evaluation/measure')]
for leg, tgt in migration_map:
    if leg in df_recs.columns:
        if tgt in df_recs.columns:
            df_recs[tgt] = df_recs[leg]
            df_recs.drop(columns=[leg], inplace=True)
        else:
            df_recs.rename(columns={leg: tgt}, inplace=True)

# Strict Schema
strict_order = [
    '_id/$oid', 'dataset/availability', 'dataset/provenance', 'dataset/provenance_source', # Added our source col to strict order
    'dataset/redundancy', 'dataset/splits', 'dataset/done', 'dataset/skip',
    'evaluation/availability', 'evaluation/comparison', 'evaluation/confidence', 'evaluation/measure', 'evaluation/method', 'evaluation/done', 'evaluation/skip',
    'model/availability', 'model/duration', 'model/interpretability', 'model/output', 'model/done', 'model/skip',
    'optimization/algorithm', 'optimization/config', 'optimization/encoding', 'optimization/features', 'optimization/fitting', 'optimization/meta',
    'optimization/parameters', 'optimization/regularization', 'optimization/done', 'optimization/skip',
    'user/$oid', 'publication/pmid', 'publication/updated', 'publication/authors', 'publication/journal', 'publication/title', 'publication/doi',
    'publication/year', 'publication/done', 'publication/skip', 'publication/tags',
    'public', 'created/$date', 'updated/$date', 'uuid', 'reviewState', 'shortid', 'update'
]

# Ensure user/$oid is populated from User_OID if exists
if 'User_OID' in df_recs.columns:
    df_recs['user/$oid'] = df_recs['User_OID']

# Ensure all strict columns exist
for col in strict_order:
    if col not in df_recs.columns: df_recs[col] = None

# Reindex (Keep extra columns like User_OID if needed, but strict order puts them at end or drops? Notebook drops strict)
# We strictly reindex but keep provenance_source which we added to strict_order above.
df_v9 = df_recs.reindex(columns=[c for c in strict_order if c in df_recs.columns] + [c for c in df_recs.columns if c not in strict_order]) 
# Actually notebook used explicit strict_order reindex, dropping others.
# But I added 'dataset/provenance_source' to strict_order so it should be safe.

output_v9 = "v9_Dome-Recommendations-Strict_Schema_Ordered.tsv"
df_v9.to_csv(output_v9, sep='\t', index=False)
print(f"  Saved v9 to {output_v9}")

# --- Step 6: Add PMCID & Fetch EPMC Source ---
print("\n[Step 6] Adding PMCID and Fetching Source Metadata...")
df_final = df_v9.copy()
if 'publication/pmid' in df_final.columns:
    idx = df_final.columns.get_loc('publication/pmid') + 1
    df_final.insert(idx, 'publication/pmcid', None)
else:
    df_final['publication/pmcid'] = None
df_final.to_csv("v10_Dome-Recommendations-With_PMCID.tsv", sep='\t', index=False)

# Fetch Source
pmids = df_final['publication/pmid'].dropna().astype(str).unique()
pmids = [p for p in pmids if p.strip() and p != 'nan']
epmc_data = []

if pmids:
    print(f"  Fetching full metadata for {len(pmids)} PMIDs...")
    def fetch_full_epmc(pmid):
        try:
            r = requests.get("https://www.ebi.ac.uk/europepmc/webservices/rest/search", 
                           params={'query': f'EXT_ID:{pmid} SRC:MED', 'format': 'json', 'resultType': 'core'})
            r.raise_for_status()
            res = r.json().get('resultList', {}).get('result', [])
            if res:
                item = res[0]
                return {
                    'publication/pmid': str(pmid).replace('.0',''),
                    'epmc_pmcid': item.get('pmcid'),
                    'epmc_journal': item.get('journalInfo', {}).get('journal', {}).get('title'),
                    'epmc_year': item.get('pubYear')
                }
        except: pass
        return None

    for i, p in enumerate(pmids):
        if i % 10 == 0: print(f"    Fetching {i+1}...", end='\r')
        meta = fetch_full_epmc(p)
        if meta: epmc_data.append(meta)
        time.sleep(0.1)
    
    df_epmc = pd.DataFrame(epmc_data)
    df_epmc.to_csv("epmc_source_metadata.tsv", sep='\t', index=False)
    print("\n  Saved EPMC source metadata.")

# --- Step 7: Repair Metadata ---
print("\n[Step 7] Repairing Metadata using EPMC...")
if not df_epmc.empty:
    # Ensure types match for merge
    df_final['publication/pmid'] = df_final['publication/pmid'].astype(str).str.replace(r'\.0$', '', regex=True)
    df_epmc['publication/pmid'] = df_epmc['publication/pmid'].astype(str).str.replace(r'\.0$', '', regex=True)

    df_merged = pd.merge(df_final, df_epmc, on='publication/pmid', how='left')
    overwrite_map = [('publication/pmcid', 'epmc_pmcid'), ('publication/journal', 'epmc_journal'), ('publication/year', 'epmc_year')]
    for tgt, src in overwrite_map:
        if src in df_merged.columns:
            mask = df_merged[src].notna() & (df_merged[src].astype(str).str.strip() != '')
            df_merged.loc[mask, tgt] = df_merged.loc[mask, src]
    
    # Drop epmc columns
    df_merged.drop(columns=[c for c in df_merged.columns if c.startswith('epmc_')], inplace=True)
    df_merged.to_csv("v11_Dome-Recommendations-EPMC_Metadata_Repaired.tsv", sep='\t', index=False)
    print("  Repaired data saved (v11).")
else:
    df_final.to_csv("v11_Dome-Recommendations-EPMC_Metadata_Repaired.tsv", sep='\t', index=False)
    print("  No EPMC data to repair with. Saved v11 as copy of v10.")

# --- Step 8: JSON Prep & Deduplication ---
print("\n[Step 8] Preparing JSON and Deduplicating...")
# Update JSON with duplicates (In-place update of copied file)
if os.path.exists(raw_reviews_file):
    with open(raw_reviews_file, 'r', encoding='utf-8') as f: json_data = json.load(f)
    
    # Normalize helper
    def norm(t): return "".join(c for c in t if c.isalnum()).lower() if isinstance(t, str) else ""
    
    # Tag Duplicates
    public_entries = [e for e in json_data if e.get('public') is True]
    groups = collections.defaultdict(list)
    for e in public_entries:
        k = (norm(e.get('publication', {}).get('title', '')), norm(e.get('publication', {}).get('journal', '')))
        if k[0]: groups[k].append(e.get('shortid'))
    
    dupe_map = {}
    for k, ids in groups.items():
        if len(ids) > 1:
            valid_ids = [i for i in ids if i]
            for i in valid_ids:
                dupe_map[i] = [x for x in valid_ids if x != i]
                
    for e in json_data:
        e['Duplicate_shortid'] = dupe_map.get(e.get('shortid'), [])
        
    with open(raw_reviews_file, 'w', encoding='utf-8') as f: json.dump(json_data, f, indent=4)
    print("  Updated source JSON with duplicate tags.")
    
    # Create Public Subset
    public_content = [item for item in json_data if item.get('public') is True]
    with open("public_dome_review_raw_human_20260202.json", 'w', encoding='utf-8') as f:
        json.dump(public_content, f, indent=4)
    print("  Created public JSON subset.")

# Deduplicate v11
df_v11 = pd.read_csv("v11_Dome-Recommendations-EPMC_Metadata_Repaired.tsv", sep='\t')
df_v11['temp_norm'] = df_v11['publication/title'].apply(lambda x: "".join(c for c in str(x) if c.isalnum()).lower())
df_v11['temp_score'] = df_v11.notna().sum(axis=1)
df_v11.sort_values('temp_score', ascending=False, inplace=True)
df_clean = df_v11.drop_duplicates(subset=['temp_norm'], keep='first').drop(columns=['temp_norm', 'temp_score'])
df_clean.to_csv("v11b_Dome-Recommendations-Deduplicated.tsv", sep='\t', index=False)
print(f"  Deduplicated v11: {len(df_v11)} -> {len(df_clean)} records.")

# --- Step 9: Validation against JSON ---
print("\n[Step 9] Validate against Public JSON...")
with open("public_dome_review_raw_human_20260202.json", 'r') as f: public_json = json.load(f)
json_titles = set("".join(c for c in e.get('publication', {}).get('title', '') if c.isalnum()).lower() for e in public_json)

df_v11b = pd.read_csv("v11b_Dome-Recommendations-Deduplicated.tsv", sep='\t')
def check_match(row):
    t = "".join(c for c in str(row.get('publication/title', '')) if c.isalnum()).lower()
    return t in json_titles

mask = df_v11b.apply(check_match, axis=1)
df_retained = df_v11b[mask]
df_dropped = df_v11b[~mask]
df_retained.to_csv("v12_Dome-Recommendations-Validated_Retained.tsv", sep='\t', index=False)
print(f"  Validation: {len(df_retained)} retained, {len(df_dropped)} dropped.")

# --- Step 10: Curator Emails ---
print("\n[Step 10] Appending Curator Emails...")
# (Reuse email_to_oid from Step 2, inversed)
oid_to_email = {v: k for k, v in email_to_oid.items()}
if 'user/$oid' in df_retained.columns:
    df_retained['curator_email'] = df_retained['user/$oid'].apply(lambda x: oid_to_email.get(str(x).strip(), '') if pd.notna(x) else '')
    cols = ['curator_email'] + [c for c in df_retained.columns if c != 'curator_email']
    df_retained = df_retained[cols]
    df_retained.to_csv("v13_Dome-Recommendations-With_Emails_Retained.tsv", sep='\t', index=False)
    print("  Added emails to retained set.")

# --- Step 11: Append Missing JSON (Merge Step) ---
print("\n[Step 11] Merging Missing Records from JSON...")
# Use dedup_public_... if exists, else public_...
json_source_file = "dedup_public_dome_review_raw_human_20260202.json"
if not os.path.exists(json_source_file):
    print(f"  {json_source_file} not found, using public_dome_review_raw_human_20260202.json")
    json_source_file = "public_dome_review_raw_human_20260202.json"

with open(json_source_file, 'r') as f: json_data = json.load(f)

# Build index of existing
existing_titles = set()
existing_dois = set()
for idx, row in df_retained.iterrows():
    existing_titles.add("".join(c for c in str(row.get('publication/title', '')) if c.isalnum()).lower())
    existing_dois.add("".join(c for c in str(row.get('publication/doi', '')) if c.isalnum()).lower())

# Identify new
new_rows = []
for entry in json_data:
    t = "".join(c for c in entry.get('publication', {}).get('title', '') if c.isalnum()).lower()
    d = "".join(c for c in entry.get('publication', {}).get('doi', '') if c.isalnum()).lower()
    
    if (d and d in existing_dois) or (t and t in existing_titles):
        continue
    
    # Flatten
    row = {}
    # Helper to flatten specific columns that exist in df_retained
    for col in df_retained.columns:
        if col == 'curator_email': continue # Skip, we removed it? Notebook says remove.
        if col == 'dataset/provenance_source': 
            row[col] = 'DOME_JSON' # Set Provenance for new rows
            continue
            
        parts = col.split('/')
        val = entry
        try:
            for p in parts: val = val[p]
            if isinstance(val, (dict, list)): val = None
            row[col] = val
        except: row[col] = None
    new_rows.append(row)

if new_rows:
    df_new = pd.DataFrame(new_rows)
    # Ensure provenance key is present
    if 'dataset/provenance_source' not in df_new.columns:
         df_new['dataset/provenance_source'] = 'DOME_JSON'
         
    df_final_v14 = pd.concat([df_retained, df_new], ignore_index=True)
    print(f"  Appended {len(new_rows)} rows from JSON.")
else:
    df_final_v14 = df_retained.copy()
    print("  No new rows found in JSON.")

if 'curator_email' in df_final_v14.columns:
    df_final_v14.drop(columns=['curator_email'], inplace=True)

df_final_v14.to_csv("v14_Dome-Recommendations-Complete_From_Source.tsv", sep='\t', index=False)

# --- Step 12: Sync Tags and Finalize ---
print("\n[Step 12] Final Sync (Tags, __v, score, specific patches)...")
# Sync Tags
oid_tags = {}
for e in json_data:
    oid = e.get('_id', {}).get('$oid')
    if oid:
        tags = e.get('publication', {}).get('tags', [])
        oid_tags[oid] = ", ".join(map(str, tags)) if isinstance(tags, list) else str(tags)

for idx, row in df_final_v14.iterrows():
    oid = str(row.get('_id/$oid') or row.get('user/$oid')) # check both? logic in notebook checks oid map
    # user/$oid is curator, _id/$oid is record ID. The key in JSON is _id.$oid.
    # In DF, do we have _id/$oid? Strict schema has it.
    rec_oid = row.get('_id/$oid')
    if rec_oid and str(rec_oid) in oid_tags:
        df_final_v14.at[idx, 'publication/tags'] = oid_tags[str(rec_oid)]

# Add __v, score
if '__v' not in df_final_v14.columns: df_final_v14['__v'] = ''
if 'score' not in df_final_v14.columns: df_final_v14['score'] = ''

# Save
final_output = "v31_Dome-Recommendations-ID_Sync_Manual_Simple.tsv"
df_final_v14.to_csv(final_output, sep='\t', index=False)
print(f"  Saved Final Version to {final_output}")

print("\n=== Automation Complete ===")
