import json
from collections import defaultdict
import pandas as pd
from pathlib import Path

DATA_PATH = Path(__file__).parent.parent / 'data' / 'processed_buyer_leads.json'


def read_json_file(filepath):

    """
    Open JSON file and read through the data.
    """

    with open(filepath, 'r', encoding='utf-8') as file:
        content = file.read()            
    try:
        dataframe = json.loads(content)
        return dataframe

    # printing errors in lines
    except json.JSONDecodeError as e:
        print("Error at line:", e.lineno, "col:", e.colno)
        print("Error:", e.msg)


def consolidate_fields(dataframe):
    """
    Normalize data into cleaned up entries. We want to eventually have a catalog of these fields:
    - company
    - description
    - website
    - keywords
    - products: name, ingredients

    Currently values appear with multiple formats
    """
    new_data = []

    for data in dataframe:
        # Normalize company name
        name = (
            data.get('company_name') or
            data.get('company') or
            data.get('name')
        )
        if not name:
            return None  # Skip if no name
        
        

        # Normalize description/info/desc/keywords/labels/tags
        description = (
            data.get('description') or
            data.get('desc') or
            data.get('info') or
            ''
        )
  
        
        # Combine all keywords/labels/tags fields into a ; separated string
        keywords = [
            data.get('keywords', ''),
            data.get('labels', ''),
            data.get('tags', ''),
        ]
        keywords = '; '.join(filter(None, keywords)).lower()
        
        # Normalize URL/site
        website = (
            data.get('website') or
            data.get('site') or
            data.get('url') or
            ''
        )

        
         # Normalize products list (unify items/product_list/products)
        products = data.get('products') or data.get('items') or data.get('product_list') or []
        
    
        normalized_products = []
        for p in products:
            product_name = p.get('name', '').strip()
            if not product_name:
                continue
            
            ingredients = [ing.lower() for ing in p.get('ingredients', []) if ing]
            normalized_products.append({'name': product_name.lower(), 'ingredients': ingredients})


        new_data.append(
            {
                'name': name.strip(),
                'description': description.strip(),
                'keywords': keywords,
                'website': website,
                'products': normalized_products
            }
        )
    return new_data
        
        

def deduplicate_by_row_completeness(df, key='name'):
    """
        Check which row has more complete products and keywords for duplicates.
        We could also merge rows if necessary for a more complicated approach, but for the exercise we take the first solution
    """

    def completeness_score(row):
       
        num_products = len(row['products']) if isinstance(row['products'], list) else 0
        num_keywords = len([k for k in row['keywords'].split(';') if k.strip()]) if isinstance(row['keywords'], str) else 0
        return num_products + num_keywords

    # Add a temporary column for scoring
    df['score'] = df.apply(completeness_score, axis=1)

    # For each group, keep the row with the highest score
    deduped_df = df.loc[df.groupby(key)['score'].idxmax()].copy()

    # Drop the temp column
    deduped_df.drop(columns=['score'], inplace=True)

    return deduped_df.sort_index()

# Scoring function
def cornstarch_score(row):

    """
    For this approach I use direct keywords and associated keywords.

    If a direct keyword is found in the company name/website or description we give 3 points
    If a direct keyword is found in a product name or ingredients we give 1 points
    Associated keywords give half of the points according to the logic above.
    
    """
    cornstarch_keywords = ['cornstarch', 'corn-starch', 'corn starch', 'maize starch', 'maize powder', 'starch']
    cornstarch_associations = [
        'thickener', 'binder', 'cornmeal', 'flour',
        'cream', 'custard', 'dry shampoo', 'absorbent', 'bakery', 'powder',
        'mattifying', 'pudding', 'dessert', 'syrup'
    ]

    def contains(text, terms):
        if not isinstance(text, str):
            return False
        text = text.lower()
        return any(term in text for term in terms)

    score = 0

    # Company name
    if contains(row.get('name', ''), cornstarch_keywords):
        score += 3
    elif contains(row.get('name', ''), cornstarch_associations):
        score += 1.5

    # Keywords
    if contains(row.get('keywords', ''), cornstarch_keywords):
        score += 3
    elif contains(row.get('keywords', ''), cornstarch_associations):
        score += 1.5

    # Description
    if contains(row.get('description', ''), cornstarch_keywords):
        score += 3
    elif contains(row.get('description', ''), cornstarch_associations):
        score += 1

    # Website
    if contains(row.get('website', ''), cornstarch_keywords):
        score += 3
    elif contains(row.get('website', ''), cornstarch_associations):
        score += 1

    # Products
    for product in row.get('products', []):
        if contains(product.get('name', ''), cornstarch_keywords):
            score += 1
            break
        elif contains(product.get('name', ''), cornstarch_associations):
            score += 0.5
            break

        for ing in product.get('ingredients', []):
            if contains(ing, cornstarch_keywords):
                score += 1
                break
            elif contains(ing, cornstarch_associations):
                score += 0.5
                break

    return score




if __name__ == "__main__":
    data = read_json_file(DATA_PATH)
    new_df = consolidate_fields(data)
    df = pd.DataFrame(new_df)

    df = deduplicate_by_row_completeness(df)
    df['cornstarch_score'] = df.apply(cornstarch_score, axis=1)
    df = df.sort_values(by='cornstarch_score', ascending=False)

    print(df)
