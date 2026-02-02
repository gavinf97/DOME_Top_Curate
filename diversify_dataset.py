import os
import json
import shutil
import glob
import random

# Paths
HUMAN_EVAL_DIR = "30_human_evaluation"
UNUSED_DIR = "30_human_evaluation_unused"
RAW_REVIEWS_PATH = "DOME_Registry_JSON_Files/dome_review_raw_human_20260128.json"
COPILOT_DIR = "Copilot_v0_Processed_2025-12-04_Updated_Metadata"
MAIN_PDF_DIR = "DOME_Registry_PMC_PDFs"
SUPP_DIR = "DOME_Registry_PMC_Supplementary"
USERS_PATH = "DOME_Registry_JSON_Files/dome_users_20260130.json"

# Load Data
print("Loading data...")
try:
    with open(RAW_REVIEWS_PATH, 'r') as f:
        raw_reviews = json.load(f)

    with open(USERS_PATH, 'r') as f:
        users = json.load(f)
except Exception as e:
    print(f"Error loading source files: {e}")
    exit(1)

# Map OID to User Name
oid_to_user = {}
for u in users:
    if '_id' in u and '$oid' in u['_id']:
        oid_to_user[u['_id']['$oid']] = u.get('name', 'Unknown')

# Map DOI to Review Entry
doi_to_review = {}
for entry in raw_reviews:
    doi = entry.get('publication', {}).get('doi', '').strip()
    if doi:
        doi_to_review[doi] = entry

# 1. Identify Keepers
if not os.path.exists(HUMAN_EVAL_DIR):
    print(f"Error: {HUMAN_EVAL_DIR} not found.")
    exit(1)
    
current_folders = sorted([f for f in os.listdir(HUMAN_EVAL_DIR) if f.startswith("PMC")])
keepers_pmc = current_folders[:11] # Indices 0-10
toreplace_pmc = current_folders[11:]

print(f"Keepers ({len(keepers_pmc)}): {keepers_pmc}")
print(f"To Replace ({len(toreplace_pmc)}): {toreplace_pmc}")
if not toreplace_pmc:
    print("No folders to replace. Exiting.")
    exit(0)

# Analyze Keepers
keeper_stats = {'journals': [], 'curators': [], 'gigascience': 0}
for pmc in keepers_pmc:
    # Read existing human json
    json_path = os.path.join(HUMAN_EVAL_DIR, pmc, f"{pmc}_human.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
                journal = data.get('publication/journal', 'Unknown')
                doi = data.get('publication/doi', '').strip()
                
                user_oid = ""
                if doi in doi_to_review:
                    user_oid = doi_to_review[doi].get('user', {}).get('$oid', '')
                
                keeper_stats['journals'].append(journal)
                keeper_stats['curators'].append(user_oid)
                if "GigaScience" in journal:
                    keeper_stats['gigascience'] += 1
        except Exception as e:
            print(f"Error reading {json_path}: {e}")

print("Keeper Stats:", keeper_stats)

# 2. Build Candidate Pool
candidates = []
copilot_files = glob.glob(os.path.join(COPILOT_DIR, "*.json"))

print("Building candidate pool...")
for cf in copilot_files:
    pmcid = os.path.basename(cf).replace(".json", "")
    
    # Check if already in keepers
    if pmcid in keepers_pmc:
        continue
        
    # Check PDF
    pdf_path = os.path.join(MAIN_PDF_DIR, f"{pmcid}_main.pdf")
    if not os.path.exists(pdf_path):
        continue
        
    # Get Metadata from Copilot JSON
    try:
        with open(cf, 'r') as f:
            c_data = json.load(f)
            doi = c_data.get('publication/doi', '').strip()
    except:
        continue
        
    if not doi or doi not in doi_to_review:
        continue
        
    human_entry = doi_to_review[doi]
    journal = human_entry.get('publication', {}).get('journal', 'Unknown')
    user_oid = human_entry.get('user', {}).get('$oid', '')
    
    candidates.append({
        'pmcid': pmcid,
        'doi': doi,
        'journal': journal,
        'curator_oid': user_oid,
        'copilot_path': cf,
        'pdf_path': pdf_path,
        'human_entry': human_entry
    })

print(f"Found {len(candidates)} valid candidates.")

# 3. Selection Logic
selected = []
needed_count = 30 - len(keepers_pmc)
needed_giga = max(0, 5 - keeper_stats['gigascience'])

# Constants / Constraints
KONSTANTINOS_OID = "6683a4267089c469b417f3b5"
STYLIANI_OID = "6683a52c7089c469b417f6bf" # Found via context
SOROUSH_OID = "65e73fdb92c76639b8e309f3" # Found via context 
GAVIN_OID = "665a01aa7089c469b4646267"

# Helper to find IDs dynamically if possible
for uid, name in oid_to_user.items():
    if "Styliani" in name: STYLIANI_OID = uid
    if "Gavin" in name: GAVIN_OID = uid
    
# Check Soroush by email if name fails
for u in users:
    if "soroush" in u.get("email", ""):
        if '_id' in u: SOROUSH_OID = u['_id']['$oid']

print(f"Constraints: Konstantinos={KONSTANTINOS_OID}, Styliani={STYLIANI_OID}, Soroush={SOROUSH_OID}, Gavin={GAVIN_OID}")

# Helper to count total usages of a curator across keepers and selected
def count_usages(curator_oid, selected_list, keepers_list):
    cnt = keepers_list.count(curator_oid)
    cnt += sum(1 for s in selected_list if s['curator_oid'] == curator_oid)
    return cnt

# A. Gavin Priority (Ensure at least 1 more if possible)
gavin_candidates = [c for c in candidates if c['curator_oid'] == GAVIN_OID]
if gavin_candidates:
    # Pick one immediately
    pick = gavin_candidates[0]
    selected.append(pick)
    # keeper_stats['curators'].append(pick['curator_oid']) - Don't append to keepers_stats directly, track separately
    # needed_count -= 1  <-- REMOVE THIS DECREMENT
    if "GigaScience" in pick['journal']:
        needed_giga -= 1

# B. Select GigaScience if needed
giga_candidates = [c for c in candidates if "GigaScience" in c['journal'] and c not in selected]
# Filter Giga candidates to minimize curator overlap if possible
giga_candidates.sort(key=lambda x: (x['curator_oid'] in keeper_stats['curators']), reverse=False)

while needed_giga > 0 and giga_candidates:
    pick = giga_candidates.pop(0)
    
    # Validation
    if pick['curator_oid'] == KONSTANTINOS_OID: continue
    
    # Check Limits for Styliani/Soroush
    curr_count = count_usages(pick['curator_oid'], selected, keeper_stats['curators'])
    if pick['curator_oid'] in [STYLIANI_OID, SOROUSH_OID] and curr_count >= 2:
        continue

    if pick not in selected:
        selected.append(pick)
        needed_giga -= 1
        # keeper_stats['curators'].append(pick['curator_oid']) 

def score_candidate(c, current_curators, current_journals):
    score = 0
    oid = c['curator_oid']
    
    # Diversity Bonuses
    if oid not in current_curators: score += 10
    if c['journal'] not in current_journals: score += 5
    
    # Penalties
    if "GigaScience" in c['journal']: score -= 20
    if oid == KONSTANTINOS_OID: score -= 1000
    
    # Hard Limits (via heavy penalty)
    if oid == STYLIANI_OID and current_curators.count(STYLIANI_OID) >= 2: score -= 500
    if oid == SOROUSH_OID and current_curators.count(SOROUSH_OID) >= 2: score -= 500
        
    score += random.random()
    return score

remaining_slots = needed_count - len(selected) # Recalculate based on Gavin selection

while remaining_slots > 0:
    # Re-evaluate scores (greedy approach)
    # Reconstruct current curators list for scoring function
    # Note: keeper_stats['curators'] holds initial keepers + those we appended manually
    # We should just use a fresh reconstruction to be safe and clear
    # Actually, we didn't append to keeper_stats['curators'] in loop A (Gavin loop)? 
    # Ah, I commented that out: # keeper_stats['curators'].append(pick['curator_oid'])
    # So we need to be careful.
    
    current_curators_list = keeper_stats['curators'] + [s['curator_oid'] for s in selected]
    current_journals = set(keeper_stats['journals'] + [s['journal'] for s in selected])
    
    # Filter candidates already selected
    available = [c for c in candidates if c not in selected]
    
    if not available:
        print("Run out of candidates!")
        break
        
    # FIX: Use correct variable name 'current_curators_list'
    available.sort(key=lambda x: score_candidate(x, current_curators_list, current_journals), reverse=True)
    
    pick = available[0]
    selected.append(pick)
    remaining_slots -= 1

print(f"Selected {len(selected)} new papers.")
print("New Journals:", [s['journal'] for s in selected])

# 4. Replacement Execution
os.makedirs(UNUSED_DIR, exist_ok=True)

# A. Move old folders
for pmc in toreplace_pmc:
    src = os.path.join(HUMAN_EVAL_DIR, pmc)
    dst = os.path.join(UNUSED_DIR, pmc)
    if os.path.exists(src):
        print(f"Moving {pmc} to unused...")
        if os.path.exists(dst): 
            shutil.rmtree(dst)
        shutil.move(src, dst)

# B. Create and Populate new folders
for item in selected:
    pmcid = item['pmcid']
    folder_path = os.path.join(HUMAN_EVAL_DIR, pmcid)
    os.makedirs(folder_path, exist_ok=True)
    
    # 1. Main PDF
    shutil.copy(item['pdf_path'], os.path.join(folder_path, f"{pmcid}_main.pdf"))
    
    # 2. Copilot JSON
    shutil.copy(item['copilot_path'], os.path.join(folder_path, f"{pmcid}_copilot.json"))
    
    # 3. Human JSON (Construct from raw entry)
    raw = item['human_entry']
    flat_human = {}
    
    for section in ["publication", "dataset", "optimization", "model", "evaluation"]:
        if section in raw:
            for k, v in raw[section].items():
                 flat_human[f"{section}/{k}"] = v
                 
    with open(os.path.join(folder_path, f"{pmcid}_human.json"), 'w') as f:
        json.dump(flat_human, f, indent=2)
        
    # 4. Supplementary
    # Check if folder exists in SUPP_DIR/PMCID
    supp_path = os.path.join(SUPP_DIR, pmcid)
    if os.path.exists(supp_path):
        for f in os.listdir(supp_path):
            # Copy PDFs that are NOT the main pdf (sometimes main pdfs are inside too)
            if f.lower().endswith(".pdf") and "main.pdf" not in f:
                shutil.copy(os.path.join(supp_path, f), os.path.join(folder_path, f))

print("Diversity Update Complete.")
