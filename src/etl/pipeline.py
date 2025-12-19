import pandas as pd
import os
from datetime import timedelta

class DataPipeline:
    def __init__(self):
        self.SCHEMA = {
            'invoice_id': str,
            'entity_name': str,
            'invoice_date': 'datetime64[ns]',
            'due_date': 'datetime64[ns]',
            'amount': float,
            'currency': str,
            'type': str,
            'status': str
        }
        
    def load_csv(self, file_path: str) -> pd.DataFrame:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        try:
            df = pd.read_csv(file_path)
            print(f"Loaded {len(df)} records from {file_path}")
            return df
        except Exception as e:
            raise Exception(f"Error loading CSV: {str(e)}")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df_clean = df.copy()
        for col in ['invoice_date', 'due_date']:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        df_clean['entity_name'] = df_clean['entity_name'].astype(str).str.strip().str.upper()
        mask_missing_due = df_clean['due_date'].isna()
        
        if mask_missing_due.any():
            print(f"Found {mask_missing_due.sum()} records with missing due dates. Defaulting to net 30 days bucket")
            df_clean.loc[mask_missing_due, 'due_date'] = \
                df_clean.loc[mask_missing_due, 'invoice_date'] + timedelta(days=30)
        df_clean.dropna(subset=['invoice_id', 'invoice_date', 'amount'], inplace=True)
        
        if 'currency' in df_clean.columns:
            rates = {'USD': 1.0, 'EUR': 1.1, 'GBP': 1.25}
            def convert_curr(row):
                rate = rates.get(row['currency'], 1.0)
                return row['amount'] * rate
            df_clean['amount_usd'] = df_clean.apply(convert_curr, axis=1)
            
        return df_clean

    def validate_schema(self, df: pd.DataFrame) -> bool:
        missing_cols = [col for col in self.SCHEMA.keys() if col not in df.columns]
        if missing_cols:
            print(f"Schema Validation Failed: Missing columns {missing_cols}")
            return False
        return True

    def run_phase_1(self, input_path: str, output_path: str):
        print(f"--- Starting Pipeline for {input_path} ---")
        raw_df = self.load_csv(input_path)
        clean_df = self.clean_data(raw_df)
        if self.validate_schema(clean_df):
            clean_df.to_csv(output_path, index=False)
            print(f"Successfully processed and saved to {output_path}")
            return clean_df
        else:
            print("Pipeline aborted due to schema validation failure.")
            return None

