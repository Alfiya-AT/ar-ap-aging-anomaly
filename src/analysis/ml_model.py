import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import numpy as np

class MLAnomalyDetector:
    
    def __init__(self, contamination=0.05):
        self.model = IsolationForest(contamination=contamination, random_state=42)
        self.scaler = StandardScaler()
        
    def train_predict(self, df: pd.DataFrame) -> pd.DataFrame:
        df_ml = df.copy()
        
        df_ml['month'] = df_ml['invoice_date'].dt.month
        df_ml['dow'] = df_ml['invoice_date'].dt.dayofweek
        
        features = ['amount', 'month', 'dow']
        
        X = df_ml[features].fillna(0)
        X_scaled = self.scaler.fit_transform(X)
        
        df_ml['ml_pred'] = self.model.fit_predict(X_scaled)
        df_ml['anomaly_score'] = self.model.decision_function(X_scaled)
        
        anomalies = df_ml[df_ml['ml_pred'] == -1].copy()
        
        if not anomalies.empty:
            anomalies['anomaly_reason'] = 'Statistical Outlier (ML)'
            anomalies['severity'] = 'Medium'
            anomalies.loc[anomalies['anomaly_score'] < -0.2, 'severity'] = 'High'
            
        return anomalies

