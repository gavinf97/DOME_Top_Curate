# DOI to Metadata Converter

This tool takes a Digital Object Identifier (DOI) and retrieves standardized publication metadata using the NCBI E-utilities and Europe PMC API.

## Usage

1. **Install Dependencies:**
   Ensure you have Python installed and the `requests` library:
   ```bash
   pip install requests
   ```

2. **Run the Script:**
   Provide the DOI (or a string containing it) as a command-line argument:
   ```bash
   python DOI_EPMC_Metadata_to_JSON.py "10.1038/s41586-020-2649-2"
   ```

   It handles messy inputs (URLs, text) automatically:
   ```bash
   python DOI_EPMC_Metadata_to_JSON.py "https://doi.org/10.1038/s41586-020-2649-2"
   ```

## Output

The tool prints the metadata to the console and saves a JSON file (e.g., `metadata_32939066.json`) in the same directory.

### Metadata Format
```json
{
    "publication/title": "Title of the paper",
    "publication/authors": "Author One, Author Two",
    "publication/journal": "Journal Name",
    "publication/year": "2023",
    "publication/pmid": "12345678",
    "publication/pmcid": "PMC1234567",
    "publication/doi": "10.1000/xyz"
}
```
