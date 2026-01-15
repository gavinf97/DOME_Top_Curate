import re
import requests
import json
import sys
import time

def clean_and_extract_doi(input_string):
    """
    Extracts and standardizes a DOI from a potentially messy input string.
    Handles URLs, prefixes, and whitespace.
    """
    if not input_string:
        return None
        
    s = input_string.strip()
    
    # 1. URL Decoding (in case of encoded chars)
    try:
        from urllib.parse import unquote
        s = unquote(s)
    except:
        pass
        
    # 2. Regex to find the DOI pattern
    # Looks for '10.' followed by 4+ digits, a slash, and then non-whitespace chars
    # This captures standard DOIs like 10.1000/xyz
    doi_regex = r'(10\.\d{4,9}/[-._;()/:A-Za-z0-9]+)'
    
    match = re.search(doi_regex, s)
    if match:
        raw_doi = match.group(1)
        # Remove trailing punctuation that might have been captured (like a period at end of sentence)
        raw_doi = raw_doi.rstrip('.,;)')
        return raw_doi
    
    return None

def get_pmid_from_ncbi(doi):
    """
    Uses NCBI E-utilities (esearch) to find a PMID for a cleaned DOI.
    """
    if not doi:
        return None
        
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        "db": "pubmed",
        "term": f"{doi}[AID]", # Searching by Article ID matches DOIs well
        "retmode": "json",
        "tool": "doi_to_json_script",
        "email": "example@example.com" # Ideally should be configured
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        id_list = data.get("esearchresult", {}).get("idlist", [])
        
        if id_list:
            # Return the first match
            return id_list[0]
        
        # If no result with [AID], try a broader search
        params["term"] = doi
        response = requests.get(base_url, params=params, timeout=10)
        data = response.json()
        id_list = data.get("esearchresult", {}).get("idlist", [])
        
        if id_list:
            return id_list[0]
            
    except Exception as e:
        print(f"Warning: NCBI lookup failed for DOI {doi}: {e}")
        
    return None

def get_europe_pmc_metadata(pmid):
    """
    Fetches publication metadata from Europe PMC using the PMID.
    """
    if not pmid:
        return None
        
    api_url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    query = f"ext_id:{pmid} src:med"
    
    params = {
        'query': query,
        'format': 'json',
        'resultType': 'core'
    }
    
    try:
        response = requests.get(api_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        results = data.get('resultList', {}).get('result', [])
        
        if results:
            item = results[0]
            
            # Form authors string
            author_list = item.get('authorList', {}).get('author', [])
            if author_list:
                authors = ", ".join([f"{a.get('lastName', '')} {a.get('firstName', '')}".strip() for a in author_list])
            else:
                authors = item.get('authorString', '')
                
            # Construct flattened JSON object
            metadata = {
                "publication/title": item.get('title', ''),
                "publication/authors": authors,
                "publication/journal": item.get('journalInfo', {}).get('journal', {}).get('title', ''),
                "publication/year": item.get('pubYear', ''),
                "publication/pmid": item.get('pmid', ''),
                "publication/pmcid": item.get('pmcid', ''),
                "publication/doi": item.get('doi', '')
            }
            
            return metadata
            
    except Exception as e:
        print(f"Warning: Europe PMC lookup failed for PMID {pmid}: {e}")
        
    return None

def main():
    if len(sys.argv) < 2:
        print("Usage: python DOI_EPMC_Metadata_to_JSON.py <DOI_STRING>")
        print("Example: python DOI_EPMC_Metadata_to_JSON.py 'https://doi.org/10.1038/nature123'")
        # Default for demonstration only if run without args in an IDE
        # sys.exit(1)
        raw_input = "10.1038/s41586-020-2649-2" 
    else:
        raw_input = sys.argv[1]

    print(f"Input: {raw_input}")
    
    # 1. Clean DOI
    doi = clean_and_extract_doi(raw_input)
    if not doi:
        print("Error: Could not extract a valid DOI structure from input.")
        sys.exit(1)
        
    print(f"Cleaned DOI: {doi}")
    
    # 2. Get PMID
    pmid = get_pmid_from_ncbi(doi)
    if not pmid:
        print(f"Error: Could not find a PMID for DOI {doi} using NCBI E-utilities.")
        sys.exit(1)
        
    print(f"Found PMID: {pmid}")
    
    # 3. Get Metadata
    metadata = get_europe_pmc_metadata(pmid)
    if not metadata:
        print(f"Error: Could not retrieve metadata from Europe PMC for PMID {pmid}.")
        sys.exit(1)
        
    # 4. Output Result
    print("\n--- Retrieved Metadata ---")
    json_output = json.dumps(metadata, indent=4)
    print(json_output)
    
    # Optional: Save to file
    # Ensure output is saved in the same directory as the script if running from elsewhere, 
    # or just use relative path which now means "current working directory"
    output_filename = f"metadata_{pmid}.json"
    
    with open(output_filename, 'w') as f:
        f.write(json_output)
    print(f"\nSaved to file: {output_filename}")

if __name__ == "__main__":
    main()
