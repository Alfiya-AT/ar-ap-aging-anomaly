import pandas as pd
from typing import List, Dict

class RuleBasedDetector:
    def detect_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        anomalies = []
        
        duplicates = df[df.duplicated(subset=['entity_name', 'amount', 'invoice_date'], keep=False)]
        if not duplicates.empty:
            for idx, row in duplicates.iterrows():
                anomalies.append({
                    **row.to_dict(),
                    'anomaly_reason': 'Potential Duplicate',
                    'severity': 'High'
                })
        
        threshold = df['amount'].quantile(0.98)
        high_value = df[df['amount'] > threshold]
        for idx, row in high_value.iterrows():
            anomalies.append({
                **row.to_dict(),
                'anomaly_reason': f'Unusually High Amount (>{threshold:.2f})',
                'severity': 'Medium'
            })
            
        df['weekday'] = df['invoice_date'].dt.dayofweek
        weekend_invoices = df[df['weekday'].isin([5, 6])]
        for idx, row in weekend_invoices.iterrows():
             anomalies.append({
                **row.to_dict(),
                'anomaly_reason': 'Invoice Dated on Weekend',
                'severity': 'Low'
            })
            
        if anomalies:
            anomaly_df = pd.DataFrame(anomalies)
            return anomaly_df
        else:
            return pd.DataFrame()

