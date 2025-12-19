import pandas as pd
from datetime import datetime

class AgingEngine:
    @staticmethod
    def calculate_aging(df: pd.DataFrame, reference_date: datetime = None) -> pd.DataFrame:
        if reference_date is None:
            reference_date = datetime.now()
            
        df_out = df.copy()
        df_out['due_date'] = pd.to_datetime(df_out['due_date'])
        df_out['days_overdue'] = (reference_date - df_out['due_date']).dt.days
        
        def assign_bucket(days):
            if days <= 0:
                return 'Current'
            elif days <= 30:
                return '1-30 Days'
            elif days <= 60:
                return '31-60 Days'
            elif days <= 90:
                return '61-90 Days'
            else:
                return '90+ Days'
                
        df_out['aging_bucket'] = df_out['days_overdue'].apply(assign_bucket)
        
        bucket_order = ['Current', '1-30 Days', '31-60 Days', '61-90 Days', '90+ Days']
        df_out['aging_bucket'] = pd.Categorical(df_out['aging_bucket'], categories=bucket_order, ordered=True)
        
        return df_out

    @staticmethod
    def get_aging_summary(df: pd.DataFrame) -> pd.DataFrame:
        summary = df.groupby('aging_bucket')['amount'].sum().reset_index()
        summary.columns = ['Bucket', 'Total Amount']
        return summary

