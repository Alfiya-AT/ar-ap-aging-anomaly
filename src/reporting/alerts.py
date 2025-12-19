import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import os

class AlertSystem:
    
    def __init__(self, smtp_server='smtp.gmail.com', smtp_port=587, sender_email='alfiyat391@gmail.com'):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.password = os.getenv("SMTP_PASSWORD", "mfzy oufa lfnc ssmd")
        
    def generate_report(self, anomalies: pd.DataFrame) -> str:
        """Generates a text summary of anomalies."""
        if anomalies.empty:
            return "No anomalies detected."
            
        high_risk = anomalies[anomalies['severity'] == 'High']
        
        report = f"Anomaly Detection Report\n"
        report += f"Total Anomalies: {len(anomalies)}\n"
        report += f"High Risk Items: {len(high_risk)}\n\n"
        
        report += "Top 5 High Risk Items:\n"
        for _, row in high_risk.head(5).iterrows():
            report += f"- {row['entity_name']} | ID: {row['invoice_id']} | Amount: ${row['amount']:.2f} | Reason: {row['anomaly_reason']}\n"
            
        return report

    def send_alert(self, recipient: str, anomalies: pd.DataFrame):
        """
        Sends an email with the anomaly report.
        """
        if anomalies.empty:
            print("No anomalies to report. Skipping email.")
            return

        report_body = self.generate_report(anomalies)
        
        msg = MIMEMultipart()
        msg['From'] = self.sender_email
        msg['To'] = recipient
        msg['Subject'] = f"URGENT: {len(anomalies)} Financial Anomalies Detected"
        
        msg.attach(MIMEText(report_body, 'plain'))
        
        try:
            # Connect to SMTP Server
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls() # Secure the connection
            
            # Login
            if not self.password:
                 raise ValueError("SMTP Password not found. Please set SMTP_PASSWORD env var or provide it.")
                 
            server.login(self.sender_email, self.password)
            
            # Send Email
            server.send_message(msg)
            server.quit()
            
            print(f"--- EMAIL SENT SUCCESSFULLY to {recipient} ---")
            
        except Exception as e:
            print(f"Failed to send email: {e}")
            raise e
