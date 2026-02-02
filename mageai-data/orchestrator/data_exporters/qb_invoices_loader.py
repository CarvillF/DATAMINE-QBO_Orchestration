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
    if df.empty:
        print("No data found in this chunk. Skipping export.")
        return

    start_time = time.time()
    
    # Connection logic
    pg_password = get_secret_value('POSTGRES_PASSWORD')
    pg_user = get_secret_value('POSTGRES_USER')
    pg_db = get_secret_value('POSTGRES_DB')
    pg_host = get_secret_value('POSTGRES_HOST')
    pg_port = get_secret_value('POSTGRES_PORT')    
    db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    
    engine = create_engine(db_url)
    metadata = MetaData(schema='raw')
    table = Table('qb_invoices', metadata,
        Column('id', String, primary_key=True),
        Column('payload', JSONB),
        Column('ingested_at_utc', DateTime),
        Column('extract_window_start_utc', DateTime),
        Column('extract_window_end_utc', DateTime),
        Column('page_number', Integer),
        Column('request_payload', JSONB)
    )

    # Upsert logic
    records = df.to_dict(orient='records')
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

    # Registrar por bloque: Filas insertadas/actualizadas y duración
    duration = time.time() - start_time
    print(f"Rows processed (Upserted): {row_count}")
    print(f"Export duration: {duration:.2f} seconds")