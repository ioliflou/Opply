import pandas as pd

from scripts.processed_buyer_leads import (
    read_json_file,
    consolidate_fields,
    deduplicate_by_row_completeness,
    cornstarch_score
)

# Sample input mimicking the JSON structure
sample_data = [
    {
        "company_name": "Cornstarch Corp",
        "description": "Used as a thickener in bakery products",
        "keywords": "thickener; binder",
        "website": "http://cornstarch.com",
        "products": [
            {"name": "Corn Starch", "ingredients": ["corn", "starch"]}
        ]
    },
    {
        "company": "Flour Mill",
        "desc": "Flour and baking supplies",
        "labels": "flour; bakery",
        "site": "http://flourmill.com",
        "product_list": [
            {"name": "All purpose flour", "ingredients": []}
        ]
    },
    {
        "name": "Empty Entry",
        "keywords": "",
        "products": []
    }
]

def test_consolidate_fields():
    normalized = consolidate_fields(sample_data)
    assert isinstance(normalized, list)
    assert all('name' in item for item in normalized)
    assert normalized[0]['name'] == "Cornstarch Corp"
    assert 'corn starch' in [p['name'] for p in normalized[0]['products']]

def test_deduplicate_by_row_completeness():
    normalized = consolidate_fields(sample_data)
    df = pd.DataFrame(normalized)
    
    # Add a duplicate row with less complete data
    duplicate = {
        'name': 'Cornstarch Corp',
        'description': '',
        'keywords': '',
        'website': '',
        'products': []
    }
    df = pd.concat([df, pd.DataFrame([duplicate])], ignore_index=True)
    
    deduped = deduplicate_by_row_completeness(df)
    assert 'Cornstarch Corp' in deduped['name'].values
    # Check only one row per name after deduplication
    assert (deduped['name'] == 'Cornstarch Corp').sum() == 1

def test_cornstarch_score():
    normalized = consolidate_fields(sample_data)
    df = pd.DataFrame(normalized)
    
    scores = df.apply(cornstarch_score, axis=1)
    assert scores.iloc[0] > scores.iloc[1]  # Cornstarch Corp should score higher than Flour Mill
    assert scores.iloc[2] == 0  # Empty entry should score 0

def test_full_pipeline():
    normalized = consolidate_fields(sample_data)
    df = pd.DataFrame(normalized)
    df = deduplicate_by_row_completeness(df)
    df['cornstarch_score'] = df.apply(cornstarch_score, axis=1)
    
    # Basic assertions on final output
    assert 'cornstarch_score' in df.columns
    assert df['cornstarch_score'].max() > 0
    assert not df['name'].isnull().any()
