# Opply
Data Engineering Case study

# Buyer Lead Processing

This script processes and scores buyer leads based on product and keyword associations with cornstarch or similar substances.

## Features

- Normalizes messy JSON input into structured fields
- Deduplicates entries based on row completeness
- Calculates a custom cornstarch relevance score based on keyword matching.



### Requirements and how to run

- Python 3.7+
- pandas
- pytest


```bash
python3 -m pip install -r requirements.txt




python3 scripts/process_buyer_leads.py
python3 -m pytest tests/

