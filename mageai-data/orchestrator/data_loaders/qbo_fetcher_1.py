import pandas as pd
import requests
import time
from datetime import datetime
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

# Auxiliar functions with Logging

def get_auth_headers(logger):
    # Phase: Auth
    logger.info("Auth: Requesting new access token via Refresh Token...")
    try:
        url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': get_secret_value('QBO_REFRESH_TOKEN')
        }
        auth = (get_secret_value('QBO_CLIENT_ID'), get_secret_value('QBO_CLIENT_SECRET'))
        resp = requests.post(url, data=payload, auth=auth)
        resp.raise_for_status()
        logger.info("Auth: Access token obtained successfully.")
        return {'Authorization': f'Bearer {resp.json()["access_token"]}', 'Accept': 'application/json'}
    except Exception as e:
        logger.error(f"Auth: Failed to retrieve token. Error: {str(e)}")
        raise

def fetch_with_retry(url, headers, logger, retries=6):
    for i in range(retries):
        resp = requests.get(url, headers=headers)
        if resp.status_code == 429:
            wait_time = 2 ** (i + 1)
            logger.warning(f"API Limit: 429 Too Many Requests. Retry {i+1}/{retries} in {wait_time}s.")
            time.sleep(wait_time) 
            continue
        
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Extraction: HTTP Error {resp.status_code} for URL {url}")
            raise e
            
        return resp.json()
    
    logger.error("Extraction: Circuit Breaker - Max retries exceeded.")
    raise Exception("Max retries exceeded")

# Data extractor (Dynamic Child)

@data_loader
def load_chunk(chunk_data, *args, **kwargs):
    # Logging: Initialize logger
    logger = kwargs.get('logger')
    
    start_time = time.time()
    q_start = chunk_data['q_start']
    q_end = chunk_data['q_end']
    
    logger.info(f"--- Starting Chunk {chunk_data['index']}/{chunk_data['total']}: {q_start} ---")
    
    # OAuth 2.0: Token refresh per execution/tramo
    headers = get_auth_headers(logger)
    
    ENTITY = "Customer"
    realm_id = get_secret_value('QBO_REALM_ID')
    base_url = "https://sandbox-quickbooks.api.intuit.com" if get_secret_value('QBO_ENTORNO') == 'sandbox' else "https://quickbooks.api.intuit.com"
    
    all_records = []
    start_pos = 1
    max_res = 1000
    page_count = 0

    # Phase: Extraction
    try:
        while True:
            query = f"SELECT * FROM {ENTITY} WHERE MetaData.LastUpdatedTime >= '{q_start}' AND MetaData.LastUpdatedTime < '{q_end}' STARTPOSITION {start_pos} MAXRESULTS {max_res}"
            url = f"{base_url}/v3/company/{realm_id}/query?query={query}"
            
            data = fetch_with_retry(url, headers, logger)
            items = data.get('QueryResponse', {}).get(ENTITY, [])
            
            if not items: 
                logger.info(f"Extraction: No items found on page {page_count + 1} (StartPos: {start_pos}). Stopping.")
                break
            
            page_count += 1
            item_count = len(items)
            # Page metrics
            logger.info(f"Extraction: Page {page_count} retrieved {item_count} items.")

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

    except Exception as e:
        logger.error(f"Extraction: Critical failure in chunk {q_start}. Error: {str(e)}")
        raise

    # Validation
    # Detect unexpected empty days (Regression Check)
    if len(all_records) == 0:
        logger.warning(f"Validation: [ALERT] Chunk {q_start} returned 0 records. If this date is expected to have data, this is a regression.")
    else:
        logger.info(f"Validation: Chunk {q_start} extraction passed volumetry check (>0 items).")

    # Final metrics per chunk
    duration = time.time() - start_time
    logger.info(f"--- Chunk Summary: {q_start} ---")
    logger.info(f"Metrics: {{'pages_read': {page_count}, 'rows_fetched': {len(all_records)}, 'duration_seconds': {duration:.2f}}}")

    return pd.DataFrame(all_records)