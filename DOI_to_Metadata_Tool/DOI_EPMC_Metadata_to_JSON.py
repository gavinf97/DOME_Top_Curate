import re
import requests
import json
import sys
import time
import xml.etree.ElementTree as ET

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

def get_biorxiv_metadata(doi):
    """
    Fetches metadata from BioRxiv or MedRxiv using the DOI.
    """
    for server in ['biorxiv', 'medrxiv']:
        # BioRxiv API url structure: details/[server]/[doi]
        url = f"https://api.biorxiv.org/details/{server}/{doi}"
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                continue
                
            data = response.json()
            messages = data.get('messages', [{}])[0]
            
            if messages.get('status') == 'ok':
                collection = data.get('collection', [])
                if collection:
                    # Take the most recent version
                    item = collection[-1]
                    
                    return {
                        "publication/title": item.get('title', ''),
                        "publication/authors": item.get('authors', ''),
                        "publication/journal": f"{server.capitalize()} (Preprint)",
                        "publication/year": item.get('date', '').split('-')[0] if item.get('date') else '',
                        "publication/pmid": '',
                        "publication/pmcid": '',
                        "publication/doi": doi
                    }
        except Exception as e:
            print(f"Warning: {server} lookup failed for DOI {doi}: {e}")
            
    return None

def get_arxiv_metadata(doi):
    """
    Fetches metadata from arXiv using the DOI.
    """
    api_url = "http://export.arxiv.org/api/query"
    
    # Try to extract arXiv ID from DOI if it follows the pattern 10.48550/arXiv.1706.03762
    # or simple regex for arXiv.ID inside string
    arxiv_id_match = re.search(r'arXiv\.([\d\.]+)', doi, re.IGNORECASE)
    
    if arxiv_id_match:
        arxiv_id = arxiv_id_match.group(1)
        params = {
            "id_list": arxiv_id,
            "start": 0,
            "max_results": 1
        }
    else:
        params = {
            "search_query": f"doi:{doi}",
            "start": 0,
            "max_results": 1
        }
    
    try:
        response = requests.get(api_url, params=params, timeout=10)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            # Namespace map for Atom
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            entry = root.find('atom:entry', ns)
            
            if entry is not None:
                title = entry.find('atom:title', ns).text
                title = title.strip().replace('\n', ' ') if title else ''
                
                published = entry.find('atom:published', ns).text
                year = published[:4] if published else ''
                
                authors_list = []
                for author in entry.findall('atom:author', ns):
                    name = author.find('atom:name', ns).text
                    if name:
                        authors_list.append(name)
                authors = ", ".join(authors_list)
                
                return {
                    "publication/title": title,
                    "publication/authors": authors,
                    "publication/journal": "arXiv (Preprint)",
                    "publication/year": year,
                    "publication/pmid": '',
                    "publication/pmcid": '',
                    "publication/doi": doi
                }
    except Exception as e:
        print(f"Warning: arXiv lookup failed for DOI {doi}: {e}")
        
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
    
    metadata = None
    
    # 2. Try NCBI/Europe PMC first
    pmid = get_pmid_from_ncbi(doi)
    if pmid:
        print(f"Found PMID: {pmid}")
        temp_metadata = get_europe_pmc_metadata(pmid)
        if temp_metadata:
            # Verify DOI match to avoid false positives from loose NCBI search
            retrieved_doi = temp_metadata.get('publication/doi', '')
            # Simple normalization for comparison
            if retrieved_doi and doi and retrieved_doi.lower().strip() != doi.lower().strip():
                print(f"Warning: Retrieved metadata DOI ({retrieved_doi}) does not match input DOI ({doi}). Ignoring NCBI result.")
            else:
                metadata = temp_metadata
        
        if not metadata:
             print(f"Warning: Could not retrieve valid metadata from Europe PMC for PMID {pmid} or DOI mismatch.")
    else:
        print("PMID not found via NCBI E-utilities. Trying preprint servers...")

    # 3. If no metadata yet, try preprints
    if not metadata:
        # Try BioRxiv/MedRxiv
        print("Checking BioRxiv/MedRxiv...")
        metadata = get_biorxiv_metadata(doi)
        
    if not metadata:
        # Try arXiv
        print("Checking arXiv...")
        metadata = get_arxiv_metadata(doi)
        
    # 4. Output Result
    if metadata:
        print("\n--- Retrieved Metadata ---")
        json_output = json.dumps(metadata, indent=4)
        print(json_output)
        
        # Determine filename - prefer PMID if available, else DOI derived
        if metadata.get("publication/pmid"):
             output_filename = f"metadata_{metadata['publication/pmid']}.json"
        else:
             safe_doi = doi.replace('/', '_')
             output_filename = f"metadata_doi_{safe_doi}.json"
        
        with open(output_filename, 'w') as f:
            f.write(json_output)
        print(f"\nSaved to file: {output_filename}")
    else:
        print(f"Error: Could not find metadata for DOI {doi} in NCBI, Europe PMC, BioRxiv, MedRxiv, or arXiv.")
        sys.exit(1)

if __name__ == "__main__":
    main()
