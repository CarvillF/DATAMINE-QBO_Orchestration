import pandas as pd
from typing import Dict, List

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


# Data chunker (by days)
@data_loader
def generate_chunks(*args, **kwargs):
    # Configuration variables (from trigger)
    start_str = kwargs.get('fecha_inicio', '2025-09-01')
    end_str = kwargs.get('fecha_fin', '2026-02-01')
    
    # Chunking: split the range into daily intervals
    dates = pd.date_range(start=start_str, end=end_str, freq='D')
    
    chunks = []
    metadata = []
    
    for i in range(len(dates) - 1):
        q_start = dates[i].strftime('%Y-%m-%d')
        q_end = dates[i+1].strftime('%Y-%m-%d')
        
        # Data payload for the downstream child
        chunks.append({
            'q_start': q_start,
            'q_end': q_end,
            'index': i + 1,
            'total': len(dates) - 1
        })
        
        # Metadata to identify the child run in Mage UI
        metadata.append({'block_uuid': f"invoice_backfill_{q_start}"})
    
    # Return format for Mage Dynamic Blocks: [data_list, metadata_list]
    return [chunks, metadata]