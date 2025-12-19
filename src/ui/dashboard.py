import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.etl.pipeline import DataPipeline
from src.analysis.aging import AgingEngine
from src.analysis.rules import RuleBasedDetector
from src.analysis.ml_model import MLAnomalyDetector
from src.reporting.alerts import AlertSystem

st.set_page_config(page_title="AP/AR Anomaly Detector", layout="wide")

st.title("💰 AP/AR Anomaly Detection Dashboard")

st.sidebar.header("Configuration") 
data_type = st.sidebar.radio("Select Data Type", ["AP (Accounts Payable)", "AR (Accounts Receivable)"])
run_pipeline = st.sidebar.button("Run Analysis Pipeline")

@st.cache_data
def load_and_process_data(file_type):
    pipeline = DataPipeline()
    fname = 'ap_cleaned.csv' if file_type == 'AP' else 'ar_cleaned.csv'
    fpath = os.path.join('data/processed', fname)
    
    if not os.path.exists(fpath):
        return None
        
    df = pd.read_csv(fpath)
    
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])
    df['due_date'] = pd.to_datetime(df['due_date'])
    
    df = AgingEngine.calculate_aging(df)
    
    return df

def run_anomaly_detection(df):
    rule_engine = RuleBasedDetector()
    rule_anomalies = rule_engine.detect_anomalies(df)
    
    ml_engine = MLAnomalyDetector(contamination=0.05)
    ml_anomalies = ml_engine.train_predict(df)
    
    all_anomalies = pd.concat([rule_anomalies, ml_anomalies], ignore_index=True)
    
    return all_anomalies

type_code = "AP" if "Payable" in data_type else "AR"
df = load_and_process_data(type_code)

if df is None:
    st.error(f"Data not found. Please run the ETL pipeline first (python main.py).")
else:
    col1, col2, col3 = st.columns(3)
    total_amount = df['amount'].sum()
    total_count = len(df)
    overdue_amount = df[df['days_overdue'] > 0]['amount'].sum()
    
    col1.metric("Total Outstanding Amount", f"${total_amount:,.2f}")
    col2.metric("Total Invoices", total_count)
    col3.metric("Overdue Amount", f"${overdue_amount:,.2f}", delta_color="inverse")
    
    st.divider()
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Aging Buckets")
        aging_summary = AgingEngine.get_aging_summary(df)
        fig_aging = px.bar(aging_summary, x='Bucket', y='Total Amount', color='Bucket', title="Amount by Aging Bucket")
        st.plotly_chart(fig_aging, width="stretch")
        
    with c2:
        st.subheader("Invoice Trends")
        daily_trend = df.groupby('invoice_date')['amount'].sum().reset_index()
        fig_trend = px.line(daily_trend, x='invoice_date', y='amount', title="Daily Invoice Amounts")
        st.plotly_chart(fig_trend, width="stretch")

    st.divider()
    
    st.subheader("🚨 Detected Anomalies")
    
    anomalies = run_anomaly_detection(df)
    
    if not anomalies.empty:
        severity_filter = st.multiselect("Filter Severity", options=['High', 'Medium', 'Low'], default=['High', 'Medium'])
        
        show_anoms = anomalies[anomalies['severity'].isin(severity_filter)]
        
        st.warning(f"Found {len(show_anoms)} anomalies matching filters.")
        
        st.dataframe(
            show_anoms[['invoice_id', 'entity_name', 'amount', 'invoice_date', 'anomaly_reason', 'severity']]
            .sort_values(by='amount', ascending=False),
            width="stretch"
        )
        
        csv = show_anoms.to_csv(index=False).encode('utf-8')
        col_dl, col_email = st.columns([1, 1])
        with col_dl:
            st.download_button("Download Anomaly Report", data=csv, file_name="anomalies.csv", mime="text/csv")
        
        with col_email:
            # Email form inputs
            with st.expander("📧 Email Alert Configuration"):
                recipient = st.text_input("Recipient Email", value="finance.manager@example.com")
                
                if st.button("Send Email Alert"):
                    if not recipient:
                        st.error("Please provide both sender and recipient emails.")
                    else:
                        alert_sys = AlertSystem()
                        alert_sys.send_alert(recipient, show_anoms)
                        st.success(f"Email sent to {recipient}! (Check console for output)")
        
    else:
        st.success("No anomalies detected!")


