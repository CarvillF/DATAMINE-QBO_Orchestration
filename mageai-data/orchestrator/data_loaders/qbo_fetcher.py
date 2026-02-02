import pandas as pd
import requests
import time
from datetime import datetime
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

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

def fetch_with_retry(url, headers, retries=6):
    for i in range(retries):
        resp = requests.get(url, headers=headers)
        if resp.status_code == 429:
            time.sleep(2 ** (i + 1)) 
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception("Max retries exceeded")

# Data extractor (Dynamic Child)

@data_loader
def load_chunk(chunk_data, *args, **kwargs):
    start_time = time.time()
    q_start = chunk_data['q_start']
    q_end = chunk_data['q_end']
    
    print(f"--- Starting Chunk {chunk_data['index']}/{chunk_data['total']}: {q_start} ---")
    
    # OAuth 2.0: Token refresh per execution/tramo
    headers = get_auth_headers()
    
    ENTITY = "Invoice"
    realm_id = get_secret_value('QBO_REALM_ID')
    base_url = "https://sandbox-quickbooks.api.intuit.com" if get_secret_value('QBO_ENTORNO') == 'sandbox' else "https://quickbooks.api.intuit.com"
    
    all_records = []
    start_pos = 1
    max_res = 1000
    page_count = 0

    # Paginación logic
    while True:
        query = f"SELECT * FROM {ENTITY} WHERE MetaData.LastUpdatedTime >= '{q_start}' AND MetaData.LastUpdatedTime < '{q_end}' STARTPOSITION {start_pos} MAXRESULTS {max_res}"
        url = f"{base_url}/v3/company/{realm_id}/query?query={query}"
        
        data = fetch_with_retry(url, headers)
        items = data.get('QueryResponse', {}).get(ENTITY, [])
        if not items: break
        
        page_count += 1
        for item in items:
            all_records.append({
                'id': item['Id'],
                'payload': item,
                'ingested_at_utc': datetime.utcnow(),
                'extract_window_start_utc': q_start,
                'extract_window_end_utc': q_end,
                'page_number': page_count,
                'request_payload': {'query': query}
            })
        
        if len(items) < max_res: break
        start_pos += max_res

    # Registrar por bloque: Observabilidad
    duration = time.time() - start_time
    print(f"--- Chunk Summary: {q_start} ---")
    print(f"Pages read: {page_count}")
    print(f"Total rows fetched: {len(all_records)}")
    print(f"Duration: {duration:.2f} seconds")

    return pd.DataFrame(all_records)