# DOME Top Curate

A comprehensive PhD research project for automated retrieval, curation, and analysis of machine learning publications from the DOME Registry, with a focus on top journal entries across different academic domains.

[![License: CC BY 4.0](https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)

## 🎯 Project Overview

**DOME Top Curate** is an automated pipeline and curation system designed to:

1. **Download and process** complete DOME (Database of Machine Learning Experiments) Registry contents
2. **Retrieve full-text articles** including PDFs, supplementary materials, and metadata from PubMed Central
3. **Remediate failed DOI mappings** through manual verification workflows
4. **Curate and analyze** top-performing journal entries using interactive interfaces
5. **Generate comprehensive metadata** and statistical analyses

This project is part of ongoing PhD research in machine learning publication patterns, reproducibility, and academic data curation.

## 📋 Table of Contents

- [Features](#-features)
- [Project Structure](#-project-structure)
- [Installation](#-installation)
- [Usage Guide](#-usage-guide)
- [Workflow Overview](#-workflow-overview)
- [Data Pipeline](#-data-pipeline)
- [Curation Tools](#-curation-tools)
- [Output Files](#-output-files)
- [Research Context](#-research-context)
- [Contributing](#-contributing)
- [License](#-license)
- [Contact](#-contact)

## ✨ Features

### Automated Data Retrieval
- **DOME Registry API Integration**: Automatic download of all registry entries
- **DOI-to-PMCID Mapping**: Conversion of DOIs to PubMed/PMC identifiers via NCBI E-Utilities
- **Full-Text PDF Download**: Retrieval of open-access articles from Europe PMC
- **Supplementary Files**: Automated download of all human-readable supplementary materials
- **Metadata Enrichment**: Title and abstract extraction for all articles

### Manual Remediation Workflow
- **Failed DOI Detection**: Automatic identification of unmapped DOIs
- **Interactive HTML Reports**: Clickable DOI links for verification
- **TSV-Based Tracking**: Easy-to-use spreadsheet format for manual PMID/PMCID entry
- **Automatic Integration**: Re-integration of manually remediated IDs into the pipeline

### Intelligent Curation
- **Interactive Interface**: Jupyter-based UI for article classification
- **Progress Tracking**: Resume-capable session management
- **Automatic Backups**: Protection against data loss
- **Markdown Rendering**: Beautiful abstract display with formatting

### Comprehensive Analytics
- **Pipeline Success Metrics**: Detailed statistics on every processing stage
- **Visualization Suite**: Multi-panel charts showing retrieval success rates
- **Metadata Reports**: Both human-readable and machine-parseable output formats

## 📁 Project Structure

```
DOME_Top_Curate/
├── README.md                                      # This file
├── LICENSE.md                                     # CC BY 4.0 license
├── .gitignore                                     # Git exclusion rules
│
├── Download_DOME_Registry_Contents.ipynb          # Main pipeline notebook
├── Manual_DOI_Remediation_Interface.ipynb         # DOI remediation UI
├── curation.ipynb                                 # Article curation interface
├── randomiser.ipynb                               # Dataset randomization tool
│
├── connect_remote.ipynb                           # Remote database connections
├── remove_remote.ipynb                            # Remote disconnection management
│
├── Dataset/                                       # Working datasets (gitignored)
│   ├── top20_entries_across_journals.csv
│   ├── random_top20_entries_across_journals.csv
│   └── journal_top20_summary.csv
│
├── DOME_Registry_JSON_Files/                      # Raw API responses
│   ├── DOME_Registry_Contents_YYYY-MM-DD.json
│   └── flattened_DOME_Registry_Contents_*.json
│
├── DOME_Registry_TSV_Files/                       # Processed tabular data
│   ├── PMCIDs_DOME_Registry_Contents_*.tsv       # Main dataset with PMCIDs
│   ├── DOME_Metadata_Complete.tsv                # Comprehensive metadata
│   ├── DOME_Metadata_Summary.txt                 # Human-readable summary
│   ├── DOME_Metadata_Complete_Analysis.png       # Visualization charts
│   └── remediated_Failed_DOI_Mappings_*.tsv      # Manual remediation file
│
├── DOME_Registry_PMC_PDFs/                        # Downloaded full-text PDFs
│   └── PMC*_main.pdf
│
├── DOME_Registry_PMC_Supplementary/               # Organized by PMCID
│   └── PMC*/
│       ├── PMC*_main.pdf                          # Main article PDF
│       └── supplementary_files.*                  # All supp materials
│
└── DOME_Registry_Remediation/                     # Remediation workflow files
    ├── Failed_DOI_Mappings_YYYY-MM-DD.tsv        # Original failed mappings
    ├── Failed_DOI_Mappings_Report_*.html         # Interactive HTML report
    └── REMEDIATION_INSTRUCTIONS.txt              # Step-by-step guide
```

## 🚀 Installation

### Prerequisites

- **Python 3.8+**
- **Jupyter Notebook or JupyterLab**
- **Required Python packages**:
  ```bash
  pip install pandas numpy matplotlib requests ipywidgets markdown
  ```

### Setup

1. **Clone or download the repository**:
   ```bash
   cd /home/gavinfarrell/PhD_Code/DOME_Top_Curate
   ```

2. **Create necessary directories**:
   ```bash
   mkdir -p Dataset DOME_Registry_JSON_Files DOME_Registry_TSV_Files \
            DOME_Registry_PMC_PDFs DOME_Registry_PMC_Supplementary \
            DOME_Registry_Remediation
   ```

3. **Launch Jupyter**:
   ```bash
   jupyter notebook
   ```

## 📖 Usage Guide

### Complete Pipeline Execution

Open and run `Download_DOME_Registry_Contents.ipynb` sequentially:

**Block 1**: Download DOME Registry contents via API  
**Block 2**: Convert JSON to TSV format  
**Block 3**: Reorder and format TSV data  
**Block 4**: Map DOIs to PMCIDs via NCBI E-Utilities  
**Block 4.5**: Integrate manually remediated PMIDs/PMCIDs *(conditional)*  
**Block 5**: Download full-text PDFs from Europe PMC  
**Block 6**: Download supplementary files from NCBI OA  
**Block 6.5**: Organize PDFs into supplementary folders  
**Block 7**: Enrich TSV with titles and abstracts  
**Block 8**: Generate comprehensive metadata and visualizations  
**Block 9**: Identify and export failed DOI mappings  

### Manual DOI Remediation Workflow

1. **Run Block 9** to generate remediation files
2. **Open HTML report**: `DOME_Registry_Remediation/Failed_DOI_Mappings_Report_*.html`
3. **For each failed DOI**:
   - Click the DOI link to verify article existence
   - Search [PubMed](https://pubmed.ncbi.nlm.nih.gov/) using DOI or title
   - Record PMID/PMCID in `Failed_DOI_Mappings_*.tsv`
   - Update `Remediation_Status` to `RESOLVED`
4. **Save as**: `DOME_Registry_TSV_Files/remediated_Failed_DOI_Mappings_*.tsv`
5. **Re-run Block 4.5** to integrate remediated IDs
6. **Re-run Blocks 5-8** to download newly available content

### Data Randomization

Use `randomiser.ipynb` to shuffle datasets for unbiased curation:

```python
INPUT_FILE = 'Dataset/top20_entries_across_journals.csv'
SEED = None  # Use None for random shuffle, or set integer for reproducibility
```

### Interactive Curation

Use `curation.ipynb` for article classification:

1. **Configure** file paths and column names
2. **Run** the curation interface
3. **Classify** articles as positive/negative using buttons
4. **Resume** from where you left off (automatic progress tracking)

## 🔄 Workflow Overview

```
┌─────────────────────────────────────────────────────────────┐
│  1. DOME Registry API Download                              │
│     ↓ JSON file with all registry entries                   │
├─────────────────────────────────────────────────────────────┤
│  2. JSON Flattening & TSV Conversion                        │
│     ↓ Structured tabular data                               │
├─────────────────────────────────────────────────────────────┤
│  3. Data Formatting & Column Ordering                       │
│     ↓ Clean, organized dataset                              │
├─────────────────────────────────────────────────────────────┤
│  4. DOI-to-PMCID Mapping (NCBI E-Utilities)                │
│     ↓ PMCIDs for ~87% of entries                           │
├─────────────────────────────────────────────────────────────┤
│  4.5. Manual Remediation Integration (Conditional)          │
│     ↓ Additional PMCIDs from manual verification            │
├─────────────────────────────────────────────────────────────┤
│  5. Full-Text PDF Download (Europe PMC)                    │
│     ↓ Open-access PDFs                                      │
├─────────────────────────────────────────────────────────────┤
│  6. Supplementary Files Download (NCBI OA)                  │
│     ↓ All human-readable supplementary materials            │
├─────────────────────────────────────────────────────────────┤
│  6.5. PDF Organization                                       │
│     ↓ Consolidated folder structure by PMCID                │
├─────────────────────────────────────────────────────────────┤
│  7. Metadata Enrichment (Titles & Abstracts)               │
│     ↓ Complete article metadata                             │
├─────────────────────────────────────────────────────────────┤
│  8. Comprehensive Analysis & Visualization                  │
│     ↓ Statistics, charts, reports                           │
├─────────────────────────────────────────────────────────────┤
│  9. Failed Mapping Identification & Export                  │
│     ↓ Remediation workflow files                            │
└─────────────────────────────────────────────────────────────┘
```

## 📊 Data Pipeline

### Success Metrics (Typical Results)

| Stage | Success Rate | Notes |
|-------|--------------|-------|
| **DOI-to-PMCID Mapping** | ~87% | Some articles not in PubMed/PMC |
| **PDF Download** | ~75% | Depends on open-access status |
| **Supplementary Files** | ~45% | Only for OA articles with materials |
| **Title/Abstract Enrichment** | ~90% | Via Europe PMC API |
| **Full Retrieval (PDF+Supp+Meta)** | ~35% | Complete data package |

### Data Formats

- **JSON**: Raw API responses, flattened for processing
- **TSV**: Primary working format (preserves special characters better than CSV)
- **PDF**: Full-text articles (named by PMCID)
- **HTML**: Interactive remediation reports
- **PNG**: Visualization charts (high-resolution, 300 DPI)

## 🛠️ Curation Tools

### Interactive Features

- **Markdown Abstract Rendering**: Beautiful formatting with proper highlighting
- **Progress Tracking**: Automatic save/resume functionality
- **Backup System**: Prevents data loss during curation
- **Jump to Row**: Navigate to specific entries
- **Statistics Dashboard**: Real-time curation progress

### Classification Workflow

1. Review article title, journal, year, and abstract
2. Click **YES** (positive) or **NO** (negative)
3. Use **Skip** to defer decision
4. Automatic saving to separate CSV files
5. Resume anytime from last position

## 📄 Output Files

### Primary Datasets

- `PMCIDs_DOME_Registry_Contents_*.tsv`: Main dataset with all mappings
- `positive_entries.csv` / `negative_entries.csv`: Curated classifications

### Metadata Reports

- `DOME_Metadata_Complete.tsv`: Structured metrics table
- `DOME_Metadata_Summary.txt`: Detailed human-readable report
- `DOME_Metadata_Complete_Analysis.png`: 6-panel visualization

### Remediation Files

- `Failed_DOI_Mappings_*.tsv`: Template for manual verification
- `Failed_DOI_Mappings_Report_*.html`: Interactive clickable report
- `REMEDIATION_INSTRUCTIONS.txt`: Step-by-step guide

## 🔬 Research Context

This project supports PhD research investigating:

- **Reproducibility in ML Research**: Availability of code, data, and supplementary materials
- **Open Access Trends**: Patterns in full-text and supplementary file availability
- **Publication Patterns**: Journal-specific trends in machine learning research
- **Data Curation Methodologies**: Development of automated curation pipelines

### Key Research Questions

1. What percentage of ML publications provide complete reproducibility materials?
2. How does open access status affect availability of supplementary files?
3. What are the barriers to DOI-to-PMCID mapping for ML publications?
4. Can automated pipelines reduce manual curation effort while maintaining quality?

## 🤝 Contributing

This is an active PhD research project. Contributions, suggestions, and collaborations are welcome:

- **Bug Reports**: Open an issue describing the problem
- **Feature Requests**: Suggest improvements or new analyses
- **Collaborations**: Contact the researcher for partnership opportunities

## 📜 License

**DOME Top Curate** © 2024 by **Gavin Farrell** is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

You are free to:
- **Share**: Copy and redistribute the material
- **Adapt**: Remix, transform, and build upon the material

Under the terms:
- **Attribution**: Give appropriate credit and indicate changes made

See [LICENSE.md](LICENSE.md) for full legal details.

## 📧 Contact

**Gavin Farrell**  
PhD Researcher  
University of Glasgow  

For questions, collaboration inquiries, or access to datasets:
- Email: [Contact via institution]
- GitHub: [Repository URL]

## 🙏 Acknowledgments

This project utilizes data and APIs from:
- **DOME Registry**: Database of Machine Learning Experiments
- **NCBI E-Utilities**: National Center for Biotechnology Information
- **Europe PMC**: European PubMed Central
- **PubMed Central**: PMC Open Access Subset

## 📚 Citation

If you use this tool or methodology in your research, please cite:

```bibtex
@software{farrell2024dome,
  author = {Farrell, Gavin},
  title = {DOME Top Curate: Automated Pipeline for ML Publication Retrieval and Curation},
  year = {2024},
  publisher = {GitHub},
  url = {https://github.com/[username]/DOME_Top_Curate}
}
```

---

**Last Updated**: December 2024  
**Version**: 1.0.0  
**Status**: Active Development
