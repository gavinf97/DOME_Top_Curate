# DOME Top Curate

A PhD research project for curating and analyzing top journal entries across different academic domains.

## Project Overview

This project is part of PhD research focused on curating and analyzing top-performing entries across various academic journals. The system provides tools for connecting to remote data sources, processing journal data, and removing/managing remote connections.

## Project Structure

```
DOME_Top_Curate/
├── README.md                    # Project documentation
├── .gitignore                  # Git ignore rules
├── connect_remote.ipynb        # Notebook for establishing remote connections
├── remove_remote.ipynb         # Notebook for managing remote disconnections
└── Dataset/                    # Data directory (gitignored)
    ├── top20_entries_across_journals.csv
    └── journal_top20_summary.csv
```

## Features

- **Remote Connection Management**: Tools for connecting to and disconnecting from remote data sources
- **Journal Data Curation**: Processing and analysis of top journal entries
- **Top 20 Analysis**: Focused analysis on the top 20 entries across different journals
- **Data Summary Generation**: Automated summary reports for journal performance

## Setup and Installation

### Prerequisites
- Python 3.x
- Jupyter Notebook or JupyterLab
- Required Python packages (install via requirements if available)

### Getting Started

1. Clone or navigate to the project directory:
   ```bash
   cd /home/gavinfarrell/PhD_Code/DOME_Top_Curate
   ```

2. Launch Jupyter Notebook:
   ```bash
   jupyter notebook
   ```

3. Open the relevant notebooks:
   - `connect_remote.ipynb` - For establishing connections to remote data sources
   - `remove_remote.ipynb` - For managing and removing remote connections

## Usage

### Connecting to Remote Sources
Use the `connect_remote.ipynb` notebook to establish connections to remote academic databases or journal repositories.

### Managing Remote Connections
Use the `remove_remote.ipynb` notebook to safely disconnect from remote sources and clean up connections.

### Data Processing
The project processes journal data to identify and curate the top 20 entries across different academic journals, generating summary statistics and analysis.

## Data Files

The following data files are generated during processing (excluded from version control):
- `top20_entries_across_journals.csv` - Detailed data on top 20 journal entries
- `journal_top20_summary.csv` - Summary statistics and analysis

## Research Context

This project is part of ongoing PhD research in academic data curation and journal performance analysis. The focus is on identifying patterns and trends in top-performing academic publications across different domains.

## Contributing

This is a PhD research project. For questions or collaboration inquiries, please contact the researcher.

## License

CCBY 4.0
