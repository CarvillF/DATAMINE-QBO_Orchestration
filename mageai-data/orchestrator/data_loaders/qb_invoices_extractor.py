import io
import pandas as pd
import requests
import time
from datetime import datetime
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



# Auxiliar functions

def get_auth_headers():
    url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': get_secret_value('QBO_REFRESH_TOKEN')
    }
    auth = (get_secret_value('QBO_CLIENT_ID'), get_secret_value('QBO_CLIENT_SECRET'))
    resp = requests.post(url, data=payload, auth=auth)
    resp.raise_for_status()
    return {'Authorization': f'Bearer {resp.json()["access_token"]}', 'Accept': 'application/json'}

def fetch_with_retry(url, headers, retries=3):
    for i in range(retries):
        resp = requests.get(url, headers=headers)
        if resp.status_code == 429:
            print(f"Too many requests: Waiting {2 ** (i + 1)} seconds")
            time.sleep(2 ** (i + 1)) #! Exponential
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception("Max retries exceeded")




# Data extractor

@data_loader
def load_data_from_api(*args, **kwargs):
    
    # Configuration variables
    start_str = kwargs.get('fecha_inicio', '2025-09-01')
    end_str = kwargs.get('fecha_fin', '2026-02-01')
    
    ENTITY = "Invoice" 
    realm_id = get_secret_value('QBO_REALM_ID')
    base_url = "https://sandbox-quickbooks.api.intuit.com" if get_secret_value('QBO_ENTORNO') == 'sandbox' else "https://quickbooks.api.intuit.com"
    headers = get_auth_headers()
    
    all_records = []    #! For storing all the queried records
    

    # Chunking data retrival by days by iterating over a range of dates
    dates = pd.date_range(start=start_str, end=end_str, freq='D')
    total_days = len(dates) -1
    for i in range(total_days):     #! 
        q_start = dates[i].strftime('%Y-%m-%d') #! To string for the QBO query
        q_end = dates[i+1].strftime('%Y-%m-%d')
        
        print(f"[{i+1}/{total_days}] Processing date: {q_start} ...")

        start_pos = 1
        max_res = 1000  #! Max quantity of items returnable in a single request
        
        # Review of items per page
        while True:
            query = f"SELECT * FROM {ENTITY} WHERE MetaData.CreateTime >= '{q_start}' AND MetaData.CreateTime < '{q_end}' STARTPOSITION {start_pos} MAXRESULTS {max_res}"
            url = f"{base_url}/v3/company/{realm_id}/query?query={query}"
            
            data = fetch_with_retry(url, headers)
            items = data.get('QueryResponse', {}).get(ENTITY, [])
            
            if not items: break
            
            current_page = (start_pos // max_res) + 1
            item_count = len(items)
            print(f"   > Page {current_page}: Retrieved {item_count} items")


            for item in items:
                print(f"Appending item {item['Id']}")
                all_records.append({
                    'id': item['Id'],
                    'payload': item,
                    'ingested_at_utc': datetime.utcnow(),
                    'extract_window_start_utc': q_start,
                    'extract_window_end_utc': q_end,
                    'page_number': (start_pos // max_res) + 1,
                    'request_payload': {'query': query}
                })
            
            if len(items) < max_res: break
            start_pos += max_res
    
    if not all_records:
        return pd.DataFrame(columns=[
            'id', 
            'payload', 
            'ingested_at_utc', 
            'extract_window_start_utc', 
            'extract_window_end_utc', 
            'page_number', 
            'request_payload'
        ])
    
    return pd.DataFrame(all_records)


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
