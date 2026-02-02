from sqlalchemy import create_engine, Table, Column, String, Integer, DateTime, MetaData
from sqlalchemy.dialects.postgresql import JSONB, insert
from pandas import DataFrame
import time
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# Data exporter (Dynamic Child)

@data_exporter
def export_data(df: DataFrame, **kwargs):
    # Logging: Initialize logger
    logger = kwargs.get('logger')
    
    if df.empty:
        logger.warning("Load: DataFrame is empty. Skipping export phase.")
        return

    start_time = time.time()
    
    # Configuration variables
    table_name = 'qb_invoices'
    schema = 'raw'
    
    # Phase: Database Connection
    try:
        pg_password = get_secret_value('POSTGRES_PASSWORD')
        pg_user = get_secret_value('POSTGRES_USER')
        pg_db = get_secret_value('POSTGRES_DB')
        pg_host = get_secret_value('POSTGRES_HOST')
        pg_port = get_secret_value('POSTGRES_PORT')    
        db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
        
        engine = create_engine(db_url)
        metadata = MetaData(schema=schema)
    except Exception as e:
        logger.error(f"Load: DB Connection failed. Error: {str(e)}")
        raise

    # Table structure
    table = Table(table_name, metadata,
        Column('id', String, primary_key=True),
        Column('payload', JSONB),
        Column('ingested_at_utc', DateTime),
        Column('extract_window_start_utc', DateTime),
        Column('extract_window_end_utc', DateTime),
        Column('page_number', Integer),
        Column('request_payload', JSONB)
    )

    # Phase: Load (Upsert)
    records = df.to_dict(orient='records')
    row_count = 0
    
    logger.info(f"Load: Starting Batch Upsert for {len(records)} records...")
    
    try:
        with engine.begin() as conn:
            stmt = insert(table).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'payload': stmt.excluded.payload,
                    'ingested_at_utc': stmt.excluded.ingested_at_utc,
                    'extract_window_start_utc': stmt.excluded.extract_window_start_utc,
                    'extract_window_end_utc': stmt.excluded.extract_window_end_utc,
                    'page_number': stmt.excluded.page_number
                }
            )
            result = conn.execute(stmt)
            row_count = result.rowcount 
            
    except Exception as e:
        logger.error(f"Load: Transaction failed. Error: {str(e)}")
        raise

    duration = time.time() - start_time
    logger.info(f"--- Load Summary ---")
    logger.info(f"Metrics: {{'rows_upserted': {row_count}, 'rows_input': {len(records)}, 'duration_seconds': {duration:.2f}}}")