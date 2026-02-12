
from core.db_connections import get_neon_engine
from core.drive_manager import get_drive_service, download_parquet_as_df
import pandas as pd

def check_columns():
    # 1. Neon Columns
    try:
        engine = get_neon_engine()
        neon_cols = pd.read_sql('SELECT * FROM historico LIMIT 0', engine).columns.tolist()
        print("NEON COLUMNS:", neon_cols)
    except Exception as e:
        print("NEON ERROR:", e)
        neon_cols = []

    # 2. Drive Columns
    try:
        service = get_drive_service()
        # Leemos un parquet pequeño o headers
        df_drive = download_parquet_as_df(service, '2025_historico_v2.parquet', '1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t')
        drive_cols = df_drive.columns.tolist()
        print("DRIVE COLUMNS:", drive_cols)
    except Exception as e:
        print("DRIVE ERROR:", e)
        drive_cols = []
    
    # 3. Generate Mapping Suggestion
    print("\nSUGGESTED MAPPING (Neon -> Drive):")
    mapping = {}
    
    # Simple normalization strategy for matching
    drive_norm = {c.lower().replace(' ', '_'): c for c in drive_cols}
    
    for nc in neon_cols:
        if nc in drive_norm:
            mapping[nc] = drive_norm[nc]
        else:
            print(f"⚠️ No match for Neon column: {nc}")
            
    print(mapping)

if __name__ == '__main__':
    check_columns()
